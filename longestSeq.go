package redisemu

type (
	seqMatch struct {
		length  int
		matches []any
	}
	longestSeq struct {
		str1            []rune
		str2            []rune
		commonStart     int
		endX            int
		endY            int
		length1         int
		length2         int
		withMatchLength bool
		processed       []*seqMatch
	}
)

func newLongestSeq(str1, str2 string) *longestSeq {
	ls := &longestSeq{
		str1: []rune(str1),
		str2: []rune(str2),
	}

	// optimization: process common prefix and common suffix without recursion algorithm
	start := 0
	ex := len(ls.str1)
	ey := len(ls.str2)
	for start < ex && start < ey {
		if ls.str1[start] != ls.str2[start] {
			break
		}
		start++
	}

	for ex > start && ey > start {
		if ls.str1[ex-1] != ls.str2[ey-1] {
			break
		}
		ex--
		ey--
	}

	ls.commonStart = start
	ls.endX = ex
	ls.endY = ey
	ls.length1 = ex - start
	ls.length2 = ey - start

	return ls
}

func (ls *longestSeq) wasVisited(ix, iy int) *seqMatch {
	offset := (ix - ls.commonStart) + ((iy - ls.commonStart) * ls.length1)
	return ls.processed[offset]
}

func (ls *longestSeq) visit(ix, iy, length int, matches []any) {
	offset := (ix - ls.commonStart) + ((iy - ls.commonStart) * ls.length1)
	ls.processed[offset] = &seqMatch{
		length:  length,
		matches: matches,
	}
}

func (ls *longestSeq) longestSequence(minMatchLength int, withMatchLength bool) (length int, matches []any, matchText string) {
	// prepare for a search
	ls.withMatchLength = withMatchLength
	ls.processed = make([]*seqMatch, ls.length1*ls.length2)

	// search for the lcs
	length, matches = ls.nextLongestSequence(ls.endX-1, ls.endY-1)

	// add common prefix and suffix to the match list (suffix to the front, prefix to the back)
	commonEnd := len(ls.str1) - ls.endX
	if commonEnd > 0 {
		matches = ls.addMatch(matches, ls.endX, ls.endY, commonEnd-1)
		length += commonEnd
	}
	if ls.commonStart > 0 {
		matches = append(matches, ls.addMatch([]any{}, 0, 0, ls.commonStart-1)...)
		length += ls.commonStart
	}

	// walk through list of sequence matches and construct a single text string output
	// and filter the matches if minMatchLength > 0
	filteredMatches := make([]any, 0, len(matches))

	for _, sequence := range matches {
		pairs := sequence.([]any)
		str1Indexes, _ := pairs[0].([]any)

		begin := str1Indexes[0].(int)
		end := str1Indexes[1].(int) + 1

		matchText = string(ls.str1[begin:end]) + matchText

		seqLen := end - begin
		if seqLen >= minMatchLength {
			filteredMatches = append(filteredMatches, sequence)
		}
	}

	matches = filteredMatches
	return
}

func (ls *longestSeq) nextLongestSequence(ix, iy int) (length int, matches []any) {
	if ix < ls.commonStart || iy < ls.commonStart {
		return
	}

	seq := ls.wasVisited(ix, iy)
	if seq != nil {
		return seq.length, seq.matches
	}

	if ls.str1[ix] == ls.str2[iy] {
		sublength, submatches := ls.nextLongestSequence(ix-1, iy-1)
		length = 1 + sublength
		matches = ls.captureMatch(submatches, ix, iy)
	} else {
		leftLength, leftMatches := ls.nextLongestSequence(ix-1, iy)
		rightLength, rightMatches := ls.nextLongestSequence(ix, iy-1)

		if leftLength > rightLength {
			length = leftLength
			matches = leftMatches
		} else {
			length = rightLength
			matches = rightMatches
		}
	}
	ls.visit(ix, iy, length, matches)
	return
}

func (ls *longestSeq) captureMatch(matches []any, startX, startY int) []any {
	zlength := 0
	if len(matches) > 0 {
		// extend the length of the last match if both X and Y are in sequence
		lastMatch := matches[0].([]any)
		lastPairX := lastMatch[0].([]any)
		lastPairY := lastMatch[1].([]any)
		lastEndX := lastPairX[1].(int)
		lastEndY := lastPairY[1].(int)

		if lastEndX+1 == startX && lastEndY+1 == startY {
			lastStartX := lastPairX[0].(int)
			zlength = startX - lastStartX
			startX -= zlength
			startY -= zlength
			matches = matches[1:]
		}
	}

	return ls.addMatch(matches, startX, startY, zlength)
}

func (ls *longestSeq) addMatch(matches []any, startX, startY, zlength int) []any {
	// redis way of structuring output
	pair1 := []any{startX, startX + zlength}
	pair2 := []any{startY, startY + zlength}
	match := []any{pair1, pair2}
	if ls.withMatchLength {
		match = append(match, zlength+1)
	}
	return append([]any{match}, matches...)
}
