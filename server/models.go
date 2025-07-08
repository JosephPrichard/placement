package server

import "image/color"

const GroupDim = 100
const GroupLen = GroupDim * GroupDim * 3

type ServiceError struct {
	Msg  string `json:"msg"`
	Code int    `json:"code"`
}

type GroupKey struct {
	X int `json:"x"`
	Y int `json:"y"`
}

func KeyFromPoint(x int, y int) GroupKey {
	groupFromX := (x / GroupDim) * GroupDim
	groupFromY := (y / GroupDim) * GroupDim
	return GroupKey{X: groupFromX, Y: groupFromY}
}

type Draw struct {
	X   int        `json:"x"`
	Y   int        `json:"y"`
	Rgb color.RGBA `json:"rgb"`
}

type Tile struct {
	D    Draw   `json:"d"`
	Date string `json:"date"` // RFC 3339 timestamp
}

type TileGroup []byte

func GetTgOffset(x, y int) int {
	return (y * 3 * GroupDim) + (x * 3)
}

func (t TileGroup) SetTile(x, y int, rgb color.RGBA) TileGroup {
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
