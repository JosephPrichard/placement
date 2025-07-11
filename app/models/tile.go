package models

import (
	"fmt"
	"image/color"
)

var GroupDim = 100
var GroupLen = GroupDim * GroupDim * 3

type GroupKey struct {
	X int `json:"x"`
	Y int `json:"y"`
}

func (key GroupKey) String() string {
	return fmt.Sprintf("%d,%d", key.X, key.Y)
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

// SetTileOff sets the tile at the x,y offset relative to the group
func (t TileGroup) SetTileOff(xOff, yOff int, rgb color.RGBA) TileGroup {
	tg := t
	if len(tg) == 0 {
		tg = make([]byte, GroupLen)
	}
	location := GetTgOffset(xOff, yOff)
	tg[location] = rgb.R
	tg[location+1] = rgb.G
	tg[location+2] = rgb.B
	return tg
}
