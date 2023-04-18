BINARY_NAME=redis.exe
 
build:
    go build -o $(BINARY_NAME)
	signtool.exe sign /s TestingCert /fd SHA256 /tr http://timestamp.digicert.com /td SHA256 $(BINARY_NAME)

run:
    go build -o $(BINARY_NAME)
    ./$(BINARY_NAME)
 
clean:
    go clean
    rm $(BINARY_NAME)