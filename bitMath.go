package goredisemu

import (
	"math/bits"
)

func countSetBitRange(allBytes []byte, start, end int) (count int) {
	startByte := start / 8
	endByte := end / 8
	bytes := allBytes[startByte : endByte+1]

	startMask := uint8(int(0x100>>(start%8)) - 1)
	endMask := 0xFF - (uint8(0x80>>(end%8)) - 1)

	if startByte == endByte {
		mask := startMask & endMask
		count = bits.OnesCount8(bytes[0] & mask)
	} else {
		count = bits.OnesCount8(bytes[0] & startMask)
		final := endByte - startByte
		for midByte := 1; midByte < final; midByte++ {
			count += bits.OnesCount8(bytes[midByte])
		}
		count += bits.OnesCount8(bytes[final] & endMask)
	}
	return
}

func extractBitfieldByte(bytes []byte, offset int) uint8 {
	if offset >= len(bytes) {
		return 0
	} else {
		return bytes[offset]
	}
}

func extractBitfield(bytes []byte, start, end int) (value int64) {
	startByte := start / 8
	startBit := start % 8
	startMask := uint8(1) << (7 - startBit)
	startMask |= (startMask - 1)
	endByte := end / 8
	endBit := end % 8
	endShift := 7 - endBit
	endMask := uint8(1) << endShift
	endMask = ^(endMask - 1)

	if startByte == endByte {
		// single byte
		b := extractBitfieldByte(bytes, startByte)
		value = int64(b&startMask&endMask) >> endShift
	} else {
		// first byte
		b := extractBitfieldByte(bytes, startByte)
		value = int64(b & startMask)

		// middle bytes
		for w := startByte + 1; w < endByte; w++ {
			value = value << 8
			b = extractBitfieldByte(bytes, w)
			value |= int64(b)
		}

		// last byte
		value = value << (endBit + 1)
		b = extractBitfieldByte(bytes, endByte)
		value |= int64(b >> endShift)
	}

	return
}

func setBitfield(bytes []byte, start, width int, value int64) {
	firstByte := start / 8
	startBit := start % 8
	byteIndex := firstByte

	uval := uint64(value)

	// remove negative bit extension
	signBit := uint64(1) << (width - 1)
	uval &= signBit | (signBit - 1)

	// partial start byte
	var b byte
	if startBit > 0 {
		byteWidth := 8 - startBit
		if byteWidth > width {
			byteWidth = width
		}

		// extract the high bits of the value
		valShift := width - byteWidth
		val := uint8(uval >> valShift)

		// compute the bitmap mask, and place value in the proper bit position
		firstBit := 7 - startBit
		highBit := uint8(1) << firstBit
		bitmapMask := highBit | (highBit - 1)

		lastBit := firstBit - byteWidth
		if lastBit >= 0 {
			lowBit := uint8(1) << lastBit
			bitmapMask ^= lowBit | (lowBit - 1)
			val <<= (lastBit + 1)
		}

		// clear old bits and set the new bits
		b = bytes[byteIndex]
		b &= ^bitmapMask
		b |= val
		bytes[byteIndex] = b

		width -= byteWidth
		byteIndex++
	}

	// full bytes
	for width >= 8 {
		width -= 8
		bytes[byteIndex] = uint8(uval >> width)
		byteIndex++
	}

	// partial end byte
	if width > 0 {
		shift := 8 - width
		lowBit := uint8(1) << (shift - 1)
		bitmapMask := lowBit | (lowBit - 1)
		val := uint8(uval) << shift
		b = bytes[byteIndex]
		b &= bitmapMask
		b |= val
		bytes[byteIndex] = b
	}
}

func isSignedSumOverflow(a, b int64, bits int) bool {
	signBit := int64(1) << (bits - 1)
	if b > 0 {
		ceiling := signBit - 1
		return b > (ceiling - a)
	} else {
		bottom := ^(signBit - 1)
		return b < (bottom - a)
	}
}

func isUnsignedOverflow(value int64, bits int) bool {
	// bits for unsigned values is 63 or less, per redis restrictions
	highBit := uint64(1) << bits

	if value < 0 {
		value = -value
	}
	return uint64(value) >= highBit
}

func saturateValue(signed bool, value int64, bits int) int64 {
	if signed {
		signBit := uint64(1) << (bits - 1)
		valueMask := signBit - 1
		if value < 0 {
			return int64(^valueMask)
		} else {
			return int64(valueMask)
		}
	} else if value < 0 {
		return 0
	}

	// bits max is 64 for signed, 63 for unsigned, per redis
	highBit := int64(1) << bits
	return highBit - 1
}

func signExtend(value int64, bits int) int64 {
	u := uint64(value)
	signBit := uint64(1) << (bits - 1)
	if u&signBit > 0 {
		valueMask := signBit - 1
		signBits := ^valueMask
		return int64(u | signBits)
	}
	return value
}

// worker for findBit, not intended to be called directly
func findBitInByte(b byte, searchBit bool, testBit, stopBit uint8) int {
	if (searchBit && b > 0) || (!searchBit && b < 0xFF) {
		// found a byte that has the search bit
		// this loop is guaranteed to reach a match
		bitOffset := 0
		for {
			set := (b & testBit) > 0
			if set == searchBit {
				return bitOffset
			}
			testBit >>= 1
			bitOffset++
		}
	}
	return -1
}

// searches a byte array for a 1 (searchBit is true) or 0 (searchBit is false),
// in Big Endian bit order; specify all=true if startBit and endBit are the
// full range
func findBit(bytes []byte, startIndex, endIndex, width int, searchBit, noEnd bool) int {
	bits := len(bytes) * 8
	end := bits - 1

	// convert to bits and determine negative offsets
	var startBit, endBit int
	if startIndex < 0 {
		startBit = bits + (startIndex * width)
	} else {
		startBit = startIndex * width
	}
	if endIndex < 0 {
		endBit = bits + (endIndex * width)
	} else {
		endBit = endIndex * width
	}

	// enforce boundaries
	if startBit < 0 {
		startBit = 0
	} else if startBit > end {
		return -1
	}
	if endBit < startBit {
		return -1
	} else if endBit > end {
		endBit = end
	}

	// initialize indexes and positions
	startByte := startBit / 8
	endByte := endBit / 8
	index := startByte
	startOffset := 7 - (startBit % 8)
	endOffset := 7 - (endBit % 8)
	lastBit := uint8(1) << uint8(endOffset)

	// special handling of a partial start byte
	var b uint8
	if startOffset != 7 {
		firstBit := uint8(1) << startOffset
		mask := firstBit | (firstBit - 1)
		b = bytes[index]
		b &= mask
		if !searchBit {
			b |= ^mask
		}

		subOffset := 0
		if startByte == endByte {
			mask = lastBit - 1
			b &= ^mask
			if !searchBit {
				b |= mask
			}
			subOffset = findBitInByte(b, searchBit, firstBit, lastBit)
		} else {
			subOffset = findBitInByte(b, searchBit, firstBit, 0x01)
		}
		if subOffset >= 0 {
			return startBit + subOffset
		}

		// advance and align starts to the first full byte
		index++
		startByte++
		startBit = ((startBit + 7) / 8) * 8
	}

	// special decrement for a partial last byte
	fullEnd := endByte
	if lastBit != 0x01 {
		fullEnd--
	}

	// search full bytes
	for index <= fullEnd {
		b = bytes[index]
		subOffset := findBitInByte(b, searchBit, 0x80, 0x01)
		if subOffset >= 0 {
			return startBit + subOffset + ((index - startByte) * 8)
		}
		index++
	}

	// search the last partial byte
	if index == endByte {
		b = bytes[index]
		subOffset := findBitInByte(b, searchBit, 0x80, lastBit)
		if subOffset >= 0 {
			return startBit + subOffset + ((index - startByte) * 8)
		}
	}

	// not found
	if !searchBit && noEnd {
		return bits
	} else {
		return -1
	}
}
