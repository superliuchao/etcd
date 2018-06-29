package main

import (
	"fmt"
	"etcdcli/etcdstruct"
)


type Config struct {
	Key1 SubConfig1        `etcd:"key1"`
	Key2 []SubConfig2      `etcd:"key2"`
	Key3 map[string]string `etcd:"key3"`
}


type SubConfig1 struct {
	Subkey1 string `etcd:"subkey1"`
	Subkey2 int    `etcd:"subkey2"`
}


type SubConfig2 struct {
	Subkey1 int64 `etcd:"subkey1"`
	Subkey2 bool  `etcd:"subkey2"`
}

func main() {
	config := Config{
		Key1: SubConfig1{
			Subkey1: "subkey1",
			Subkey2: 123,
		},
		Key2: []SubConfig2{
			{Subkey1: 301, Subkey2: true},
			//{Subkey1: 999, Subkey2: false},
		},
		Key3: map[string]string{
			"mapkey1": "mapvalue1",
			"mapkey2": "mapvalue2",
		},
	}

	etc, err := etcdstruct.NewClient([]string{
		"192.168.101.86:2379",
	}, "teststruct", &config)

	if err != nil {
		fmt.Println(err)
		return
	}

	if err := etc.Save(); err != nil {
		fmt.Println(err)
		return
	}

	config.Key1.Subkey1 = "subkey1 changed"

	if err := etc.SaveField(&config.Key1.Subkey1); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Saved!")
}
