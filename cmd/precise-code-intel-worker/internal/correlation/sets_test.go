package correlation

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestIDSet(t *testing.T) {
	ids1 := IDSet{}
	ids1.Add("bonk")
	ids1.Add("quux")

	ids2 := IDSet{}
	ids2.Add("foo")
	ids2.Add("bar")
	ids2.Add("baz")
	ids2.AddAll(ids1)

	expected := []string{"bar", "baz", "bonk", "foo", "quux"}
	if diff := cmp.Diff(expected, ids2.Keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}

	if value, ok := ids2.Choose(); !ok {
		t.Errorf("expected a value to be chosen")
	} else {
		if value != "bar" {
			t.Errorf("unexpected chosen value. want=%s have=%s", "bar", value)
		}
	}

	ids2.Add("alpha")
	if value, ok := ids2.Choose(); !ok {
		t.Errorf("expected a value to be chosen")
	} else {
		if value != "alpha" {
			t.Errorf("unexpected chosen value. want=%s have=%s", "alpha", value)
		}
	}
}

func TestChooseEmptyIDSet(t *testing.T) {
	ids := IDSet{}
	if _, ok := ids.Choose(); ok {
		t.Errorf("unexpected ok")
	}
}

//
//
//

func TestDefaultIDSetMap(t *testing.T) {
	m := DefaultIDSetMap{}
	m.GetOrCreate("foo").Add("bar")
	m.GetOrCreate("foo").Add("baz")
	m.GetOrCreate("bar").Add("bonk")
	m.GetOrCreate("bar").Add("quux")

	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	expected := []string{"bar", "foo"}
	if diff := cmp.Diff(expected, keys); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}

	expected = []string{"bar", "baz"}
	if diff := cmp.Diff(expected, m["foo"].Keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}

	expected = []string{"bonk", "quux"}
	if diff := cmp.Diff(expected, m["bar"].Keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
}

//
//
//

func TestDisjointIDSet(t *testing.T) {
	s := DisjointIDSet{}
	s.Union("1", "2")
	s.Union("3", "4")
	s.Union("1", "3")
	s.Union("5", "6")

	if diff := cmp.Diff([]string{"1", "2", "3", "4"}, s.ExtractSet("1").Keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"1", "2", "3", "4"}, s.ExtractSet("2").Keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"1", "2", "3", "4"}, s.ExtractSet("3").Keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"1", "2", "3", "4"}, s.ExtractSet("4").Keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"5", "6"}, s.ExtractSet("5").Keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"5", "6"}, s.ExtractSet("6").Keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
}
