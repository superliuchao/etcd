package main

import (
 	cli	"etcdcli/etcd"
	"fmt"
	"strconv"
)

type test struct {
	client *cli.Client
	//clientStruct *clistruct.Client
}

//s
func NewT() *test{
	client, err := cli.NewClient([]string{"http://192.168.101.86:2379"},
				"", 0)

	if err != nil  {
		fmt.Printf("NewClient Error %v", err)
		return nil
	}

	//clistruct, errstruct := clistruct.NewClient()
	fmt.Printf("test:%v\n", client)
	return  &test{client: client}
}


/*
	Set(key string, value string, ttl int64, swapValue string, swapIndex int64) error
	SetDir(key string, ttl int64) error
	Update(key string, value string, ttl int64) error
	UpdateDir(key string, value string) error
	RM(key string, idDir bool, recursive bool,  preValue string, preIndex int64) error
	RMDir(key string) error
	Get(key string) (string, error)
	List(path string) ([] string)
	MK(key string, value string, ttl int64) error
	MKDir(key string, ttl int64) error
	Watch(key string, recursive bool, onChange OnChangeCallback) (error)
*/
func (c *test) value() {
	var err error

	err = c.client.Set("/test/set/v1", "k1", 0, "", 0)
	if (err != nil) {
		fmt.Printf("err: %v\n", err)
		return
	}

	if value, err := c.client.Get("/test/set/v1"); err == nil && value == "k1" {
		fmt.Printf("Get--[OK]\n")
	} else {
		fmt.Printf("Get--[Failed]--%v\n", err)
	}

	err = c.client.Set("/test/set/ttl100", "ttl100", 100, "", 0)
	fmt.Printf("Setttl--e:%v ", err)

	err = c.client.Set("/test/set/prevalue", "prevalue", 0, "", 0)
	err = c.client.Set("/test/set/prevalue", "prevaluenew", 0, "prevalue", 0)

	err = c.client.Set("/test/set/update", "update", 0, "", 0)
	err = c.client.Update("/test/set/update", "updatenew", 0)


	err = c.client.Set("/test/set/rm", "rm", 0, "", 0)
	err = c.client.RM("/test/set/rm", false, false, "", 0)
	fmt.Printf("rm /test/set/rm e:%v\n", err)

}

func (c *test) dir() {
	var err error
	err = c.client.MKDir("/test/dir/mkdir", 0)
	fmt.Printf("mkdir e:%v\n", err)

	err = c.client.MKDir("/test/dir/mkdirttl", 100)
	fmt.Printf("mkdir with ttl e:%v \n", err)

	err = c.client.MKDir("/test/dir/mkdirrm", 0)
	fmt.Printf("mkdirrm mkdirrm e:%v \n", err)

	err = c.client.RMDir("/test/dir/mkdirrm")
	fmt.Printf("first:delete mkdirrm  e:%v \n", err)

	err = c.client.RMDir("/test/dir/mkdirrm")
	fmt.Printf("second:delete mkdirrm  e:%v \n", err)
}


func (c *test) mk() {

	c.client.MKDir("/test/mk", 0)

	for i := 0; i < 20; i++ {
		index := "mk"
		index += strconv.Itoa(i)
		err := c.client.MK("/test/mk", index, 0, true)
		if err != nil {
			fmt.Printf("mk /test/mk/%v e:%v\n", index, err)
		}
	}

	lists, err := c.client.List("/test/mk", true)
	fmt.Printf("List--%v  e:%v\n", lists, err)
}

func (c *test) watch() {
	onchange := func(action string, path string, value string) bool {
		fmt.Printf("action:%v path:%v value:%v\n", action, path, value)
		return false
	}
	c.client.MKDir("/test/listen", 0)
	fmt.Printf("wathch into routine\n")
	err := c.client.Watch("/test/listen", true, onchange)
	fmt.Printf("wathch err:%v\n", err)

}

func  main()  {
	t := NewT()
	t.watch()
	//t.value()
	//t.dir()
	//t.mk()

}
