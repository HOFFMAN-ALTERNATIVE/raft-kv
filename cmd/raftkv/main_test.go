package main

import "testing"

func TestParsePeers(t *testing.T) {
	peers, ids, err := parsePeers("n1=http://a:1/, n2=http://b:2")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "n1" || peers["n1"] != "http://a:1" || peers["n2"] != "http://b:2" {
		t.Fatalf("got %v %v", ids, peers)
	}
	for _, bad := range []string{"", "n1", "=http://x", "n1="} {
		if _, _, err := parsePeers(bad); err == nil {
			t.Errorf("parsePeers(%q) accepted", bad)
		}
	}
}
