package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/wojtechnology/medblocks/crypt"
)

type Person struct {
	Name  string
	Phone string
}

func hello(w http.ResponseWriter, r *http.Request) {
	var response = "YOLO\nFAM\n"
	for i := 0; i < 10; i++ {
		response = response + strconv.Itoa(i) + "\n"
	}
	response = response + "wehjkgbaejlgkfahwef"
	io.WriteString(w, response)

}

func ecdsaExample() {
	priv, err := crypt.CreateKey()
	if err != nil {
		panic(err)
	}
	hash := []byte("YOLO FAM")
	r, s, err := crypt.Sign(hash, priv)
	// should return true
	fmt.Println(crypt.Verify(hash, &priv.PublicKey, r, s))
}

func mongodbExample() {
	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)

	c := session.DB("test").C("people")
	err = c.Insert(&Person{"Ale", "+55 53 8116 9639"},
		&Person{"Cla", "+55 53 8402 8510"})
	if err != nil {
		log.Fatal(err)
	}

	result := Person{}
	err = c.Find(bson.M{"name": "Ale"}).One(&result)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Phone:", result.Phone)
}

func main() {
	// http.HandleFunc("/", hello)
	// http.ListenAndServe(":8000", nil)
	// mongodbExample()
	ecdsaExample()
}
