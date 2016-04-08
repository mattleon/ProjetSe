main: kv.o kv.h main.o
	gcc main.o kv.o -o main

main.o: kv.h
	gcc -c main.c

kv.o: kv.h
	gcc -c kv.c
