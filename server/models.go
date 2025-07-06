package server

import "image/color"

const GroupDim = 100
const GroupLen = GroupDim * GroupDim * 3

type GroupKey struct {
	x int32
	y int32
}

func KeyFromPoint(x int32, y int32) GroupKey {
	groupFromX := (x / GroupDim) * GroupDim
	groupFromY := (y / GroupDim) * GroupDim
	return GroupKey{x: groupFromX, y: groupFromY}
}

type Draw struct {
	x   int32
	y   int32
	rgb color.RGBA
}

type Tile struct {
	d    Draw
	date string // RFC 3339 timestamp
}

type TileGroup []byte

func GetTgOffset(x, y int32) int32 {
	return (y * 3 * GroupDim) + (x * 3)
}

func (t TileGroup) SetTile(x, y int32, rgb color.RGBA) TileGroup {
	tg := t
	if len(tg) == 0 {
		tg = make([]byte, GroupLen)
	}
	location := GetTgOffset(x, y)
	tg[location] = rgb.R
	tg[location+1] = rgb.G
	tg[location+2] = rgb.B
	return tg
}
