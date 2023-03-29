PB = $(wildcard *.proto)
GO = $(PB:.proto=.pb.go)

all: $(GO)

%.pb.go: %.proto
	docker run -v $(CURDIR):/defs -w /defs namely/protoc-all -d . -l go --go-module-prefix github.com/ipni/go-libipni/find/pb -o .

clean:
	rm -f *.pb.go
	rm -f *.go
