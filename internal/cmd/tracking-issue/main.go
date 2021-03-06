// Command tracking-issue uses the GitHub API to maintain open tracking issues.

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/machinebox/graphql"
	"golang.org/x/oauth2"
)

func main() {
	token := flag.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub personal access token")
	org := flag.String("org", "sourcegraph", "GitHub organization to list issues from")
	dry := flag.Bool("dry", false, "If true, do not update GitHub tracking issues in-place, but print them to stdout")
	verbose := flag.Bool("verbose", false, "If true, print the resulting tracking issue bodies to stdout")

	flag.Parse()

	if err := run(*token, *org, *dry, *verbose); err != nil {
		log.Fatal(err)
	}
}

func run(token, org string, dry, verbose bool) (err error) {
	if token == "" {
		return fmt.Errorf("no -token given")
	}

	if org == "" {
		return fmt.Errorf("no -org given")
	}

	ctx := context.Background()
	cli := graphql.NewClient("https://api.github.com/graphql", graphql.WithHTTPClient(
		oauth2.NewClient(ctx, oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: token},
		))),
	)

	issues, err := listTrackingIssues(ctx, cli, org)
	if err != nil {
		return err
	}

	if len(issues) == 0 {
		log.Printf("No tracking issues found. Exiting.")
		return nil
	}

	tracking := make([]*TrackingIssue, 0, len(issues))
	for _, issue := range issues {
		tracking = append(tracking, &TrackingIssue{Issue: issue})
	}

	err = loadTrackingIssues(ctx, cli, org, tracking)
	if err != nil {
		return err
	}

	var toUpdate []*Issue
	for _, issue := range tracking {
		if updated, err := issue.UpdateWork(issue.Workloads().Markdown()); err != nil {
			log.Printf("failed to patch work section in %q %s: %v", issue.Title, issue.URL, err)
		} else if !updated {
			log.Printf("%q %s not modified.", issue.Title, issue.URL)
		} else if !dry {
			log.Printf("%q %s modified", issue.Title, issue.URL)
			toUpdate = append(toUpdate, issue.Issue)
		} else {
			log.Printf("%q %s modified, but not updated due to -dry=true.", issue.Title, issue.URL)
		}

		if verbose {
			log.Printf("%q %s body\n%s\n\n", issue.Title, issue.URL, issue.Body)
		}
	}

	if len(toUpdate) > 0 {
		return updateIssues(ctx, cli, toUpdate)
	}

	return nil
}

func updateIssues(ctx context.Context, cli *graphql.Client, issues []*Issue) (err error) {
	var q bytes.Buffer
	q.WriteString("mutation(")

	for _, issue := range issues {
		fmt.Fprintf(&q, "$issue%dInput: UpdateIssueInput!,", issue.Number)
	}

	q.Truncate(q.Len() - 1)
	q.WriteString(") {")

	for _, issue := range issues {
		fmt.Fprintf(&q, "issue%[1]d: updateIssue(input: $issue%[1]dInput) { issue { updatedAt } }\n", issue.Number)
	}

	q.WriteString("}")

	r := graphql.NewRequest(q.String())

	type UpdateIssueInput struct {
		ID   string `json:"id"`
		Body string `json:"body"`
	}

	for _, issue := range issues {
		r.Var(fmt.Sprintf("issue%dInput", issue.Number), &UpdateIssueInput{
			ID:   issue.ID,
			Body: issue.Body,
		})
	}

	return cli.Run(ctx, r, nil)
}

func patch(s, replacement, opening, closing string) (string, error) {
	start := strings.Index(s, opening)
	if start == -1 {
		return s, errors.New("could not find opening marker in issue body")
	}

	end := strings.Index(s, closing)
	if end == -1 {
		return s, errors.New("could not find closing marker in issue body")
	}

	return s[:start+len(opening)] + replacement + s[end:], nil
}

type Workloads map[string]*Workload

func (ws Workloads) Markdown() string {
	assignees := make([]string, 0, len(ws))
	for assignee := range ws {
		assignees = append(assignees, assignee)
	}

	sort.Strings(assignees)

	var b strings.Builder
	for _, assignee := range assignees {
		b.WriteString(ws[assignee].Markdown())
	}

	return b.String()
}

type Workload struct {
	Assignee     string
	Days         float64
	Issues       []*Issue
	PullRequests []*PullRequest
}

func (wl *Workload) Markdown() string {
	var b strings.Builder

	var days string
	if wl.Days > 0 {
		days = fmt.Sprintf(": __%.2fd__", wl.Days)
	}

	fmt.Fprintf(&b, "\n@%s%s\n\n", wl.Assignee, days)

	for _, issue := range wl.Issues {
		b.WriteString(issue.Markdown())

		for _, pr := range issue.LinkedPRs {
			b.WriteString("  ") // Nested list
			b.WriteString(pr.Markdown())
		}
	}

	// Put all PRs that aren't linked to issues top-level
	for _, pr := range wl.PullRequests {
		if len(pr.LinkedIssues) == 0 {
			b.WriteString(pr.Markdown())
		}
	}

	return b.String()
}

func Days(estimate string) float64 {
	d, _ := strconv.ParseFloat(strings.TrimSuffix(estimate, "d"), 64)
	return d
}

func Estimate(labels []string) string {
	const prefix = "estimate/"
	for _, label := range labels {
		if strings.HasPrefix(label, prefix) {
			return label[len(prefix):]
		}
	}
	return ""
}

var matcher = regexp.MustCompile(`https://app\.hubspot\.com/contacts/2762526/company/\d+`)

func Customer(body string) string {
	customer := matcher.FindString(body)
	if customer == "" {
		return "👩"
	}
	return "[👩](" + customer + ")"
}

func Assignee(assignees []string) string {
	if len(assignees) == 0 {
		return "Unassigned"
	}
	return assignees[0]
}

type TrackingIssue struct {
	*Issue
	Issues []*Issue
	PRs    []*PullRequest
}

func (t *TrackingIssue) UpdateWork(work string) (updated bool, err error) {
	const (
		openingMarker = "<!-- BEGIN WORK -->"
		closingMarker = "<!-- END WORK -->"
	)

	before := t.Body

	after, err := patch(t.Body, work, openingMarker, closingMarker)
	if err != nil {
		return false, err
	}

	t.Body = after
	return before != after, nil
}

func (t *TrackingIssue) Workloads() Workloads {
	workloads := map[string]*Workload{}

	workload := func(assignee string) *Workload {
		w := workloads[assignee]
		if w == nil {
			w = &Workload{Assignee: assignee}
			workloads[assignee] = w
		}
		return w
	}

	for _, pr := range t.PRs {
		w := workload(pr.Author)
		w.PullRequests = append(w.PullRequests, pr)
	}

	for _, issue := range t.Issues {
		w := workload(Assignee(issue.Assignees))

		w.Issues = append(w.Issues, issue)

		linked := issue.LinkedPullRequests(t.PRs)
		for _, pr := range linked {
			issue.LinkedPRs = append(issue.LinkedPRs, pr)
			pr.LinkedIssues = append(pr.LinkedIssues, issue)
		}

		if t.Milestone == "" || issue.Milestone == t.Milestone {
			estimate := Estimate(issue.Labels)
			w.Days += Days(estimate)
		} else {
			issue.Deprioritised = true
		}
	}

	return workloads
}

type Issue struct {
	ID         string
	Title      string
	Body       string
	Number     int
	URL        string
	State      string
	Repository string
	Private    bool
	Labels     []string
	Assignees  []string
	Milestone  string
	Author     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	ClosedAt   time.Time

	Deprioritised bool           `json:"-"`
	LinkedPRs     []*PullRequest `json:"-"`
}

func (issue *Issue) Markdown() string {
	state := " "
	if strings.EqualFold(issue.State, "closed") {
		state = "x"
	}

	estimate := Estimate(issue.Labels)

	if estimate != "" {
		estimate = "__" + estimate + "__ "
	}

	return fmt.Sprintf("- [%s] %s [#%d](%s) %s%s\n",
		state,
		issue.title(),
		issue.Number,
		issue.URL,
		estimate,
		issue.Emojis(),
	)
}

func (issue *Issue) Emojis() string {
	categories := Categories(issue.Labels, issue.Repository, issue.Body)
	return Emojis(categories)
}

func Emojis(categories map[string]string) string {
	sorted := make([]string, 0, len(categories))
	length := 0

	for _, emoji := range categories {
		sorted = append(sorted, emoji)
		length += len(emoji)
	}

	sort.Strings(sorted)

	s := make([]byte, 0, length)
	for _, emoji := range sorted {
		s = append(s, emoji...)
	}

	return string(s)
}

func has(label string, labels []string) bool {
	for _, l := range labels {
		if label == l {
			return true
		}
	}
	return false
}

func (issue *Issue) title() string {
	var title string

	if issue.Private {
		title = issue.Repository
	} else {
		title = issue.Title
	}

	// Cross off issues that were originally planned
	// for the milestone but are no longer in it.
	if issue.Deprioritised {
		title = "~" + strings.TrimSpace(title) + "~"
	}

	return title
}

func (issue *Issue) LinkedPullRequests(prs []*PullRequest) (linked []*PullRequest) {
	for _, pr := range prs {
		if strings.Contains(pr.Body, "#"+strconv.Itoa(issue.Number)) {
			linked = append(linked, pr)
		}
	}
	return linked
}

func (issue *Issue) Redact() {
	if issue.Private {
		issue.Title = "REDACTED"
		issue.Labels = RedactLabels(issue.Labels)
	}
}

func RedactLabels(labels []string) []string {
	redacted := labels[:0]
	for _, label := range labels {
		if strings.HasPrefix(label, "estimate/") || strings.HasPrefix(label, "planned/") {
			redacted = append(redacted, label)
		}
	}
	return redacted
}

type PullRequest struct {
	ID         string
	Title      string
	Body       string
	Number     int
	URL        string
	State      string
	Repository string
	Private    bool
	Labels     []string
	Assignees  []string
	Milestone  string
	Author     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	ClosedAt   time.Time
	BeganAt    time.Time // Time of the first authored commit

	LinkedIssues []*Issue `json:"-"`
}

func (pr *PullRequest) Markdown() string {
	state := " "
	if strings.EqualFold(pr.State, "merged") {
		state = "x"
	}

	return fmt.Sprintf("- [%s] %s [#%d](%s) %s\n",
		state,
		pr.title(),
		pr.Number,
		pr.URL,
		pr.Emojis(),
	)
}

func (pr *PullRequest) Emojis() string {
	categories := Categories(pr.Labels, pr.Repository, pr.Body)
	categories["pull-request"] = ":shipit:"
	return Emojis(categories)
}

func (pr *PullRequest) title() string {
	var title string

	if pr.Private {
		title = pr.Repository
	} else {
		title = pr.Title
	}

	if strings.EqualFold(pr.State, "closed") {
		title = "~" + strings.TrimSpace(title) + "~"
	}

	return title
}

func (pr *PullRequest) Redact() {
	if pr.Private {
		pr.Title = "REDACTED"
		pr.Labels = RedactLabels(pr.Labels)
	}
}

func Categories(labels []string, repository, body string) map[string]string {
	categories := make(map[string]string, len(labels))

	switch repository {
	case "sourcegraph/customer":
		categories["customer"] = Customer(body)
	case "sourcegraph/security-prs":
		categories["security"] = Emoji("security")
	}

	for _, label := range labels {
		if label == "customer" {
			categories[label] = Customer(body)
		} else if emoji := Emoji(label); emoji != "" {
			categories[label] = emoji
		}
	}

	return categories
}

func Emoji(category string) string {
	switch category {
	case "roadmap":
		return "🛠️"
	case "debt":
		return "🧶"
	case "spike":
		return "🕵️"

	case "bug":
		return "🐛"
	case "security":
		return "🔒"
	default:
		return ""
	}
}

type searchNode struct {
	Typename   string `json:"__typename"`
	ID         string
	Title      string
	Body       string
	State      string
	Number     int
	URL        string
	Repository struct {
		NameWithOwner string
		IsPrivate     bool
	}
	Author    struct{ Login string }
	Assignees struct{ Nodes []struct{ Login string } }
	Labels    struct{ Nodes []struct{ Name string } }
	Milestone struct{ Title string }
	Commits   struct {
		Nodes []struct {
			Commit struct{ AuthoredDate time.Time }
		}
	}
	CreatedAt time.Time
	UpdatedAt time.Time
	ClosedAt  time.Time
}

type search struct {
	PageInfo struct {
		EndCursor   string
		HasNextPage bool
	}
	Nodes []searchNode
}

func loadTrackingIssues(ctx context.Context, cli *graphql.Client, org string, issues []*TrackingIssue) error {
	var q bytes.Buffer
	q.WriteString("query(\n")

	type query struct {
		issue  *TrackingIssue
		count  int
		cursor string
		query  string
	}

	queries := map[string]*query{}
	for _, issue := range issues {
		if issue.Milestone == "" {
			name := "tracking" + strconv.Itoa(issue.Number)
			fmt.Fprintf(&q, "$%[1]sCount: Int!, $%[1]sCursor: String, $%[1]sQuery: String!,\n", name)
			queries[name] = &query{
				issue: issue,
				count: 100,
				query: listIssuesSearchQuery(org, "", issue.Labels, false),
			}
		} else {
			milestoned := "tracking" + strconv.Itoa(issue.Number) + "Milestoned"
			fmt.Fprintf(&q, "$%[1]sCount: Int!, $%[1]sCursor: String, $%[1]sQuery: String!,\n", milestoned)

			queries[milestoned] = &query{
				issue: issue,
				count: 100,
				query: listIssuesSearchQuery(org, issue.Milestone, issue.Labels, false),
			}

			demilestoned := "tracking" + strconv.Itoa(issue.Number) + "Demilestoned"
			fmt.Fprintf(&q, "$%[1]sCount: Int!, $%[1]sCursor: String, $%[1]sQuery: String!,\n", demilestoned)

			queries[demilestoned] = &query{
				issue: issue,
				count: 100,
				query: listIssuesSearchQuery(org, issue.Milestone, issue.Labels, true),
			}
		}
	}

	q.Truncate(q.Len() - 1) // Remove the trailing comma from the loop above.
	q.WriteString(") {")

	for query := range queries {
		q.WriteString(searchGraphQLQuery(query))
	}

	q.WriteString("}")

	for {
		r := graphql.NewRequest(q.String())

		for query, args := range queries {
			r.Var(query+"Count", args.count)
			r.Var(query+"Query", args.query)
			if args.cursor != "" {
				r.Var(query+"Cursor", args.cursor)
			}
		}

		var data map[string]search

		err := cli.Run(ctx, r, &data)
		if err != nil {
			return err
		}

		var hasNextPage bool
		for query, s := range data {
			q := queries[query]

			if s.PageInfo.HasNextPage {
				hasNextPage = true
				q.cursor = s.PageInfo.EndCursor
			} else {
				q.count = 0
			}

			issues, prs := unmarshalSearchNodes(s.Nodes)
			q.issue.Issues = append(q.issue.Issues, issues...)
			q.issue.PRs = append(q.issue.PRs, prs...)
		}

		if !hasNextPage {
			break
		}
	}

	return nil
}

func listTrackingIssues(ctx context.Context, cli *graphql.Client, org string) (all []*Issue, _ error) {
	var q strings.Builder
	q.WriteString("query($trackingCount: Int!, $trackingCursor: String, $trackingQuery: String!) {\n")
	q.WriteString(searchGraphQLQuery("tracking"))
	q.WriteString("}")

	r := graphql.NewRequest(q.String())

	r.Var("trackingCount", 100)
	r.Var("trackingQuery", fmt.Sprintf("org:%q label:tracking is:open", org))

	for {
		var data struct{ Tracking search }

		err := cli.Run(ctx, r, &data)
		if err != nil {
			return nil, err
		}

		issues, _ := unmarshalSearchNodes(data.Tracking.Nodes)
		all = append(all, issues...)

		if data.Tracking.PageInfo.HasNextPage {
			r.Var("trackingCursor", data.Tracking.PageInfo.EndCursor)
		} else {
			break
		}
	}

	return all, nil
}

func unmarshalSearchNodes(nodes []searchNode) (issues []*Issue, prs []*PullRequest) {
	for _, n := range nodes {
		switch n.Typename {
		case "PullRequest":
			pr := &PullRequest{
				ID:         n.ID,
				Title:      n.Title,
				Body:       n.Body,
				State:      n.State,
				Number:     n.Number,
				URL:        n.URL,
				Repository: n.Repository.NameWithOwner,
				Private:    n.Repository.IsPrivate,
				Assignees:  make([]string, 0, len(n.Assignees.Nodes)),
				Labels:     make([]string, 0, len(n.Labels.Nodes)),
				Milestone:  n.Milestone.Title,
				Author:     n.Author.Login,
				CreatedAt:  n.CreatedAt,
				UpdatedAt:  n.UpdatedAt,
				ClosedAt:   n.ClosedAt,
				BeganAt:    n.Commits.Nodes[0].Commit.AuthoredDate,
			}

			for _, assignee := range n.Assignees.Nodes {
				pr.Assignees = append(pr.Assignees, assignee.Login)
			}

			for _, label := range n.Labels.Nodes {
				pr.Labels = append(pr.Labels, label.Name)
			}

			prs = append(prs, pr)

		case "Issue":
			issue := &Issue{
				ID:         n.ID,
				Title:      n.Title,
				Body:       n.Body,
				State:      n.State,
				Number:     n.Number,
				URL:        n.URL,
				Repository: n.Repository.NameWithOwner,
				Private:    n.Repository.IsPrivate,
				Assignees:  make([]string, 0, len(n.Assignees.Nodes)),
				Labels:     make([]string, 0, len(n.Labels.Nodes)),
				Milestone:  n.Milestone.Title,
				Author:     n.Author.Login,
				CreatedAt:  n.CreatedAt,
				UpdatedAt:  n.UpdatedAt,
				ClosedAt:   n.ClosedAt,
			}

			for _, assignee := range n.Assignees.Nodes {
				issue.Assignees = append(issue.Assignees, assignee.Login)
			}

			for _, label := range n.Labels.Nodes {
				issue.Labels = append(issue.Labels, label.Name)
			}

			issues = append(issues, issue)
		}
	}

	return issues, prs
}

func searchGraphQLQuery(alias string) string {
	const searchQuery = `%[1]s: search(first: $%[1]sCount, type: ISSUE, after: $%[1]sCursor query: $%[1]sQuery) {
		pageInfo {
			endCursor
			hasNextPage
		}
		nodes {
			... on Issue {
				%s
			}
			... on PullRequest {
				%s
			}
		}
	}`

	return fmt.Sprintf(searchQuery,
		alias,
		searchNodeFields(false),
		searchNodeFields(true),
	)
}

func searchNodeFields(isPR bool) string {
	fields := `
		__typename
		id, title, body, state, number, url
		createdAt, closedAt
		repository { nameWithOwner, isPrivate }
		author { login }
		assignees(first: 25) { nodes { login } }
		labels(first: 25) { nodes { name } }
		milestone { title }
	`

	if isPR {
		fields += `
			commits(first: 1) { nodes { commit { authoredDate } } }
		`
	}

	return fields
}

func listIssuesSearchQuery(org, milestone string, labels []string, demilestoned bool) string {
	var q strings.Builder

	fmt.Fprintf(&q, "org:%q", org)

	if milestone != "" {
		if demilestoned {
			fmt.Fprintf(&q, ` -milestone:%q label:"planned/%s"`, milestone, milestone)
		} else {
			fmt.Fprintf(&q, " milestone:%q", milestone)
		}
	}

	for _, label := range labels {
		if label != "" && label != "tracking" {
			fmt.Fprintf(&q, " label:%q", label)
		}
	}

	return q.String()
}
