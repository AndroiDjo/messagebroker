package main

import (
	"fmt"
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
	/*strings2 := []string{"fsdgsdfsaafasdfddffhls.fsdlkgsdlkjfsd.fsdlfjlskdj",
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
	fmt.Println("custom equals ends in", end.Sub(begin))*/

	mainslice := []int{1, 2, 3, 4, 5}
	//subslice := mainslice[1:3]
	subslice := remove(mainslice, 2)
	fmt.Println(subslice)
	subslice = append(subslice, 333)
	fmt.Println(subslice)
}

func remove(s []int, i int) []int {
	s[i] = s[len(s)-1]
	// We do not need to put s[i] at the end, as it will be discarded anyway
	return s[:len(s)-1]
}
