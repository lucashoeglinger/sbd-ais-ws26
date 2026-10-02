package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Course struct {
	Name       string
	Id         string
	Instructor string
	ProgrammId int
}

func (c *Course) print() {
	fmt.Println("Name", c.Name)
	fmt.Println("Course Id", c.Id)
	fmt.Println("Instructor", c.Instructor)
	fmt.Printf("Programm Id %04d\n", c.ProgrammId)
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Please enter the course details: ")

	var str, _ = reader.ReadString('\n')

	var c_arr [5]string
	parts := strings.Split(str, ",")
	copy(c_arr[:], parts)

	programmId, err := strconv.Atoi(strings.TrimSpace(c_arr[3]))

	if err != nil {
		panic("Critical Errror has occured")
	}

	c := &Course{Name: c_arr[0], Id: c_arr[1], Instructor: c_arr[2], ProgrammId: programmId}
	c.print()
}
