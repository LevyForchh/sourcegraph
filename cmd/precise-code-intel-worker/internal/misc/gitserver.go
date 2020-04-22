package misc

// func getCommitsNear(db db.DB, repositoryID int, commit string) ([]string, error) {
// 	// TODO
// 	repoName, err := db.RepoName(context.Background(), repositoryID)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// TODO - move
// 	const MaxCommitsPerUpdate = 150 // MAX_TRAVERSAL_LIMIT * 1.5

// 	cmd := gitserver.DefaultClient.Command("git", "log", "--pretty=%H %P", commit, fmt.Sprintf("-%d", MaxCommitsPerUpdate))
// 	cmd.Repo = gitserver.Repo{Name: api.RepoName(repoName)}
// 	out, err := cmd.CombinedOutput(context.Background())
// 	if err != nil {
// 		return nil, err
// 	}

// 	return strings.Split(string(bytes.TrimSpace(out)), "\n"), nil
// }

// func parseCmomitsNear() (map[string][]string, error) {
// 	commits := map[string][]string{}

// 	for _, dude := range allDudes {
// 		line := strings.TrimSpace(dude)
// 		if line == "" {
// 			continue
// 		}

// 		parts := strings.Split(line, " ")
// 		commits[parts[0]] = parts[1:]
// 	}

// 	return commits
// }
