target:
	clear
	gcc -g -o main -Wall -pthread main.c

clean:
	rm main

run:
	./main file_in.bin 10