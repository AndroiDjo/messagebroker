package main

import (
	"fmt"
	"strings"
	"time"
)

func fastcompare(str1 string, str2 string) bool {
	result := false
	if len(str1) == len(str2) {
		result = true
		for i := 0; i < len(str1); i++ {
			if str1[i] != str2[i] {
				result = false
				break
			}
		}
	}
	return result
}

func main() {
	/*strings1 := []string{"dlshflkashdflkjsdklfhls.fsdlkgsdlkjfsd.fsdlfjlskdj",
	"fsldhskdhflkhsadlkjfjsldkjflskdhfs.dflshdglskdjsdl",
	"khfsldhflsdjf.sdfjlksjd.fjkskdlfjs.dfjkskdjkdkjfkf",
	"flskhdflksjdflkhsdhkflkasdjfksdjlkfjldkjsflsdkhslk",
	"fkshdlfkjslkdfkhalskhdflkjsdfkljsdlkfjslkdh.slkdfj",
	"khlskdkfhhksdfkjhslkdjflksjdklfhskdl.flskdhflkhsdl",
	"hflsyhelihfkhsldhfoishdfs.fhsldhfoiwoie"}*/
	strings2 := []string{"fsdgsdfsaafasdfddffhls.fsdlkgsdlkjfsd.fsdlfjlskdj",
		"fsldhgsdfasdfsdddkjfjsldkjflskdhfs.dflshdglskdjsdl",
		"khfsldhflsdjf.sdfjlksjd.fjkskdlfjs.dfjkskdjkdkjfkf",
		"flskhdflkssgsdfasd*fdfgsdfdjfksdjlkfjldkjsflsdkhslk",
		"fkshdlfkjslkdfkhalskhdflkjsdfkljsdlkfjslkdh.slkdfj",
		"khlskdkfhhksdfkjhslkd.ksdjflkhsd.kdl.flskdhflkhsdl",
		"fshdlkfhlsdhf*lksjdf.sdlkfhslkdflksdflkjsd",
		"hfakshdflkjslkdfhlksdfksj.fskdhfklsd",
		"fhlsydkflskdjflkjsdlfkhsd.fhsldhflksdflkjsdlkf",
		"hflsyhelihfkhsld#hfoishdfs.fhsldhfoiwoie",
		"fklhsdyfioshdfosjklj.hflidhfoisdh",
		"fhosiydfoijskdfhlhsd.fhsdjgfoisydfipusdpf"}

	begin := time.Now()
	fmt.Println("regular equals start")
	for i := 0; i < 1000000; i++ {
		eq := 0
		for _, s := range strings2 {
			if strings.Contains(s, "#") || strings.Contains(s, "*") {
				eq++
			}
		}
	}
	end := time.Now()
	fmt.Println("regular equals ends in", end.Sub(begin))

	begin = time.Now()
	fmt.Println("custom equals start")
	for i := 0; i < 1000000; i++ {
		eq := 0
		for _, s := range strings2 {
			if strings.ContainsAny(s, "#*") {
				eq++
			}
		}
	}
	end = time.Now()
	fmt.Println("custom equals ends in", end.Sub(begin))
}
