package converter

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestIDSet(t *testing.T) {
	ids1 := idSet{}
	ids1.add("bonk")
	ids1.add("quux")

	ids2 := idSet{}
	ids2.add("foo")
	ids2.add("bar")
	ids2.add("baz")
	ids2.addAll(ids1)

	expected := []string{"bar", "baz", "bonk", "foo", "quux"}
	if diff := cmp.Diff(expected, ids2.keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}

	if value, ok := ids2.choose(); !ok {
		t.Errorf("expected a value to be chosen")
	} else {
		if value != "bar" {
			t.Errorf("unexpected chosen value. want=%s have=%s", "bar", value)
		}
	}

	ids2.add("alpha")
	if value, ok := ids2.choose(); !ok {
		t.Errorf("expected a value to be chosen")
	} else {
		if value != "alpha" {
			t.Errorf("unexpected chosen value. want=%s have=%s", "alpha", value)
		}
	}
}

func TestChooseEmptyIDSet(t *testing.T) {
	ids := idSet{}
	if _, ok := ids.choose(); ok {
		t.Errorf("unexpected ok")
	}
}

//
//
//

func TestDefaultIDSetMap(t *testing.T) {
	m := defaultIDSetMap{}
	m.getOrCreate("foo").add("bar")
	m.getOrCreate("foo").add("baz")
	m.getOrCreate("bar").add("bonk")
	m.getOrCreate("bar").add("quux")

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
	if diff := cmp.Diff(expected, m["foo"].keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}

	expected = []string{"bonk", "quux"}
	if diff := cmp.Diff(expected, m["bar"].keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
}

//
//
//

func TestDisjointIDSet(t *testing.T) {
	s := disjointIDSet{}
	s.union("1", "2")
	s.union("3", "4")
	s.union("1", "3")
	s.union("5", "6")

	if diff := cmp.Diff([]string{"1", "2", "3", "4"}, s.extractSet("1").keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"1", "2", "3", "4"}, s.extractSet("2").keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"1", "2", "3", "4"}, s.extractSet("3").keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"1", "2", "3", "4"}, s.extractSet("4").keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"5", "6"}, s.extractSet("5").keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"5", "6"}, s.extractSet("6").keys()); diff != "" {
		t.Errorf("unexpected keys (-want +got):\n%s", diff)
	}
}
