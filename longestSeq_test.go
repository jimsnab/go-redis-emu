package redisemu

import (
	"testing"
)

type (
	testLcsRange struct {
		start int
		end   int
	}

	testLcsMatch struct {
		str1 testLcsRange
		str2 testLcsRange
		text string
	}
)

func compareSeqOutput(t *testing.T, length int, matches []any, matchText string, expected []testLcsMatch, hasLength bool) {
	compareSeqOutputEx(t, length, matches, matchText, expected, hasLength, nil)
}

func compareSeqOutputEx(t *testing.T, length int, matches []any, matchText string, expected []testLcsMatch, hasLength bool, expectedText *string) {
	if len(matches) != len(expected) {
		t.Error("sequence match length mismatch")
		return
	}

	allText := ""
	if expectedText != nil {
		allText = *expectedText
	}

	for matchIndex, match := range matches {
		expectedMatch := expected[matchIndex]
		if expectedText == nil {
			allText = expectedMatch.text + allText
		}
		matchPairs := match.([]any)

		if hasLength {
			if len(matchPairs) != 3 {
				t.Errorf("sequence match %s pair length error", expectedMatch.text)
				return
			}
		} else {
			if len(matchPairs) != 2 {
				t.Errorf("sequence match %s pair length error", expectedMatch.text)
			}
		}

		indicies1, ok := matchPairs[0].([]any)
		if !ok {
			t.Errorf("sequence match %s string index wrong type", expectedMatch.text)
			return
		}
		start, ok := indicies1[0].(int)
		if !ok {
			t.Errorf("sequence match %s start index 1 wrong type", expectedMatch.text)
			return
		}
		if start != expectedMatch.str1.start {
			t.Errorf("sequence match %s start index 1 %d is not expected %d", expectedMatch.text, start, expectedMatch.str1.start)
			return
		}
		end, ok := indicies1[1].(int)
		if !ok {
			t.Errorf("sequence match %s end index 1 wrong type", expectedMatch.text)
			return
		}
		if end != expectedMatch.str1.end {
			t.Errorf("sequence match %s end index 1 %d is not expected %d", expectedMatch.text, end, expectedMatch.str1.end)
			return
		}

		indicies2, ok := matchPairs[1].([]any)
		if !ok {
			t.Errorf("sequence match %s string index wrong type", expectedMatch.text)
			return
		}
		start, ok = indicies2[0].(int)
		if !ok {
			t.Errorf("sequence match %s start index 2 wrong type", expectedMatch.text)
			return
		}
		if start != expectedMatch.str2.start {
			t.Errorf("sequence match %s start index 2 %d is not expected %d", expectedMatch.text, start, expectedMatch.str2.start)
			return
		}
		end, ok = indicies2[1].(int)
		if !ok {
			t.Errorf("sequence match %s end index 2 wrong type", expectedMatch.text)
			return
		}
		if end != expectedMatch.str2.end {
			t.Errorf("sequence match %s end index 2 %d is not expected %d", expectedMatch.text, end, expectedMatch.str2.end)
			return
		}

		if hasLength {
			matchLength, ok := matchPairs[2].(int)
			if !ok {
				t.Errorf("sequence match %s length wrong type", expectedMatch.text)
				return
			}
			if matchLength != len(expectedMatch.text) {
				t.Errorf("sequence match %s length != %d", expectedMatch.text, len(expectedMatch.text))
				return
			}
		}
	}

	if length != len(allText) {
		t.Errorf("length %d does not match length %d of expected %s", length, len(allText), allText)
	}
	if matchText != allText {
		t.Errorf("match text %s does not match expected %s", matchText, allText)
	}
}

func TestLongestSeqEmpty(t *testing.T) {
	ls1 := newLongestSeq("", "")
	length, matches, matchText := ls1.longestSequence(0, false)
	if length != 0 || len(matches) != 0 || matchText != "" {
		t.Error("empty string test failed")
	}

	length, matches, matchText = ls1.longestSequence(0, true)
	if length != 0 || len(matches) != 0 || matchText != "" {
		t.Error("empty string test failed")
	}

	length, matches, matchText = ls1.longestSequence(1, false)
	if length != 0 || len(matches) != 0 || matchText != "" {
		t.Error("empty string test failed")
	}

	length, matches, matchText = ls1.longestSequence(1, true)
	if length != 0 || len(matches) != 0 || matchText != "" {
		t.Error("empty string test failed")
	}
}

func TestLongestSeqExact(t *testing.T) {

	expected1 := []testLcsMatch{
		{
			str1: testLcsRange{start: 0, end: 2},
			str2: testLcsRange{start: 0, end: 2},
			text: "cat",
		},
	}

	ls1 := newLongestSeq("cat", "cat")
	length, matches, matchText := ls1.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected1, false)

	length, matches, matchText = ls1.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected1, true)
}

func TestLongestSeqPrefix(t *testing.T) {

	expected1 := []testLcsMatch{
		{
			str1: testLcsRange{start: 0, end: 2},
			str2: testLcsRange{start: 0, end: 2},
			text: "cat",
		},
	}

	ls1 := newLongestSeq("cat1", "cat2")
	length, matches, matchText := ls1.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected1, false)

	length, matches, matchText = ls1.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected1, true)

	expected2 := []testLcsMatch{
		{
			str1: testLcsRange{start: 0, end: 2},
			str2: testLcsRange{start: 0, end: 2},
			text: "cat",
		},
	}

	ls2 := newLongestSeq("cat1", "cat234")
	length, matches, matchText = ls2.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected2, false)

	length, matches, matchText = ls2.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected2, true)

	expected3 := []testLcsMatch{
		{
			str1: testLcsRange{start: 0, end: 2},
			str2: testLcsRange{start: 0, end: 2},
			text: "cat",
		},
	}

	ls3 := newLongestSeq("cat123", "cat4")
	length, matches, matchText = ls3.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected3, false)

	length, matches, matchText = ls3.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected3, true)

	expected4 := []testLcsMatch{
		{
			str1: testLcsRange{start: 0, end: 2},
			str2: testLcsRange{start: 0, end: 2},
			text: "cat",
		},
	}

	ls4 := newLongestSeq("cat123", "cat")
	length, matches, matchText = ls4.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected4, false)

	length, matches, matchText = ls4.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected4, true)

	expected5 := []testLcsMatch{
		{
			str1: testLcsRange{start: 0, end: 2},
			str2: testLcsRange{start: 0, end: 2},
			text: "cat",
		},
	}

	ls5 := newLongestSeq("cat", "cat123")
	length, matches, matchText = ls5.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected5, false)

	length, matches, matchText = ls5.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected5, true)
}

func TestLongestSeqSuffix(t *testing.T) {
	expected1 := []testLcsMatch{
		{
			str1: testLcsRange{start: 1, end: 3},
			str2: testLcsRange{start: 1, end: 3},
			text: "cat",
		},
	}

	ls1 := newLongestSeq("1cat", "2cat")
	length, matches, matchText := ls1.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected1, false)

	length, matches, matchText = ls1.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected1, true)

	expected2 := []testLcsMatch{
		{
			str1: testLcsRange{start: 1, end: 3},
			str2: testLcsRange{start: 3, end: 5},
			text: "cat",
		},
	}

	ls2 := newLongestSeq("1cat", "234cat")
	length, matches, matchText = ls2.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected2, false)

	length, matches, matchText = ls2.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected2, true)

	expected3 := []testLcsMatch{
		{
			str1: testLcsRange{start: 3, end: 5},
			str2: testLcsRange{start: 1, end: 3},
			text: "cat",
		},
	}

	ls3 := newLongestSeq("123cat", "4cat")
	length, matches, matchText = ls3.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected3, false)

	length, matches, matchText = ls3.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected3, true)

	expected4 := []testLcsMatch{
		{
			str1: testLcsRange{start: 0, end: 2},
			str2: testLcsRange{start: 1, end: 3},
			text: "cat",
		},
	}

	ls4 := newLongestSeq("cat", "4cat")
	length, matches, matchText = ls4.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected4, false)

	length, matches, matchText = ls4.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected4, true)

	expected5 := []testLcsMatch{
		{
			str1: testLcsRange{start: 1, end: 3},
			str2: testLcsRange{start: 0, end: 2},
			text: "cat",
		},
	}

	ls5 := newLongestSeq("1cat", "cat")
	length, matches, matchText = ls5.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected5, false)

	length, matches, matchText = ls5.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected5, true)
}

func TestLongestSeqMiddleA(t *testing.T) {

	expected1 := []testLcsMatch{
		{
			str1: testLcsRange{start: 1, end: 1},
			str2: testLcsRange{start: 1, end: 1},
			text: "a",
		},
	}

	ls1 := newLongestSeq("ca!", "ba?")
	length, matches, matchText := ls1.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected1, false)

	length, matches, matchText = ls1.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected1, true)

	expected2 := []testLcsMatch{
		{
			str1: testLcsRange{start: 1, end: 1},
			str2: testLcsRange{start: 2, end: 2},
			text: "a",
		},
	}

	ls2 := newLongestSeq("ca!", "xba?")
	length, matches, matchText = ls2.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected2, false)

	length, matches, matchText = ls2.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected2, true)

	expected3 := []testLcsMatch{
		{
			str1: testLcsRange{start: 2, end: 2},
			str2: testLcsRange{start: 1, end: 1},
			text: "a",
		},
	}

	ls3 := newLongestSeq("xca!", "ba?")
	length, matches, matchText = ls3.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected3, false)

	length, matches, matchText = ls3.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected3, true)
}

func TestLongestSeqMiddleAt2(t *testing.T) {

	expected1 := []testLcsMatch{
		{
			str1: testLcsRange{start: 1, end: 2},
			str2: testLcsRange{start: 1, end: 2},
			text: "at",
		},
	}

	ls1 := newLongestSeq("cats", "bath")
	length, matches, matchText := ls1.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected1, false)

	length, matches, matchText = ls1.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected1, true)
}

func TestLongestSeqSingleX(t *testing.T) {

	expected1 := []testLcsMatch{
		{
			str1: testLcsRange{start: 1, end: 1},
			str2: testLcsRange{start: 1, end: 1},
			text: "x",
		},
	}

	ls1 := newLongestSeq("cxts", "bxzh")
	length, matches, matchText := ls1.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected1, false)

	length, matches, matchText = ls1.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected1, true)

	expected2 := []testLcsMatch{
		{
			str1: testLcsRange{start: 0, end: 0},
			str2: testLcsRange{start: 0, end: 0},
			text: "x",
		},
	}

	ls2 := newLongestSeq("xcts", "xbzh")
	length, matches, matchText = ls2.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected2, false)

	length, matches, matchText = ls2.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected2, true)

	expected3 := []testLcsMatch{
		{
			str1: testLcsRange{start: 3, end: 3},
			str2: testLcsRange{start: 3, end: 3},
			text: "x",
		},
	}

	ls3 := newLongestSeq("cdtx", "bazx")
	length, matches, matchText = ls3.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected3, false)

	length, matches, matchText = ls3.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected3, true)

	expected4 := []testLcsMatch{
		{
			str1: testLcsRange{start: 0, end: 0},
			str2: testLcsRange{start: 3, end: 3},
			text: "x",
		},
	}

	ls4 := newLongestSeq("xdog", "catx")
	length, matches, matchText = ls4.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected4, false)

	length, matches, matchText = ls4.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected4, true)
}

func TestLongestSeqSplit(t *testing.T) {

	expected1 := []testLcsMatch{
		{
			str1: testLcsRange{start: 3, end: 4},
			str2: testLcsRange{start: 4, end: 5},
			text: "at",
		},
		{
			str1: testLcsRange{start: 0, end: 1},
			str2: testLcsRange{start: 0, end: 1},
			text: "no",
		},
	}

	ls1 := newLongestSeq("nocats", "no-bath")
	length, matches, matchText := ls1.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected1, false)

	length, matches, matchText = ls1.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected1, true)

	expected2 := []testLcsMatch{
		{
			str1: testLcsRange{start: 7, end: 9},
			str2: testLcsRange{start: 10, end: 12},
			text: "!!!",
		},
		{
			str1: testLcsRange{start: 3, end: 4},
			str2: testLcsRange{start: 4, end: 5},
			text: "at",
		},
		{
			str1: testLcsRange{start: 0, end: 1},
			str2: testLcsRange{start: 0, end: 1},
			text: "no",
		},
	}

	ls2 := newLongestSeq("nocats0!!!*", "no-bath-by!!!")
	length, matches, matchText = ls2.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected2, false)

	length, matches, matchText = ls2.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected2, true)
}

func TestLongestSeqDocs(t *testing.T) {

	expected1 := []testLcsMatch{
		{
			str1: testLcsRange{start: 5, end: 5},
			str2: testLcsRange{start: 6, end: 6},
			text: "B",
		},
		{
			str1: testLcsRange{start: 4, end: 4},
			str2: testLcsRange{start: 4, end: 4},
			text: "A",
		},
		{
			str1: testLcsRange{start: 3, end: 3},
			str2: testLcsRange{start: 2, end: 2},
			text: "T",
		},
		{
			str1: testLcsRange{start: 2, end: 2},
			str2: testLcsRange{start: 0, end: 0},
			text: "G",
		},
	}

	ls1 := newLongestSeq("AGGTAB", "GXTXAYB")
	length, matches, matchText := ls1.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected1, false)

	length, matches, matchText = ls1.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected1, true)

	expected2 := []testLcsMatch{
		{
			str1: testLcsRange{start: 4, end: 7},
			str2: testLcsRange{start: 5, end: 8},
			text: "text",
		},
		{
			str1: testLcsRange{start: 2, end: 3},
			str2: testLcsRange{start: 0, end: 1},
			text: "my",
		},
	}

	ls2 := newLongestSeq("ohmytext", "mynewtext")
	length, matches, matchText = ls2.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected2, false)

	length, matches, matchText = ls2.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected2, true)
}

func TestLongestMin(t *testing.T) {
	expected1a := []testLcsMatch{
		{
			str1: testLcsRange{start: 4, end: 7},
			str2: testLcsRange{start: 5, end: 8},
			text: "text",
		},
	}

	allText := "mytext"

	ls1 := newLongestSeq("ohmytext", "mynewtext")
	length, matches, matchText := ls1.longestSequence(4, false)
	compareSeqOutputEx(t, length, matches, matchText, expected1a, false, &allText)

	length, matches, matchText = ls1.longestSequence(4, true)
	compareSeqOutputEx(t, length, matches, matchText, expected1a, true, &allText)

	expected1b := []testLcsMatch{
		{
			str1: testLcsRange{start: 4, end: 7},
			str2: testLcsRange{start: 5, end: 8},
			text: "text",
		},
		{
			str1: testLcsRange{start: 2, end: 3},
			str2: testLcsRange{start: 0, end: 1},
			text: "my",
		},
	}

	length, matches, matchText = ls1.longestSequence(0, false)
	compareSeqOutput(t, length, matches, matchText, expected1b, false)

	length, matches, matchText = ls1.longestSequence(0, true)
	compareSeqOutput(t, length, matches, matchText, expected1b, true)
}
