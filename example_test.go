package shrinkbolt_test

import (
	"github.com/aerth/shrinkbolt"
)

func ExampleShrinkBoltDatabase(){
	err := shrinkbolt.ShrinkBoltDatabase(
		"some.dat",
		"some.shrunken.dat",
	)
	if err != nil {
		// do something
	}
}