
GRAPHS = step-1 step-2 step-3 step-4 step-5 step-6 step-7 step-8 step-8-bis step-8-ter step-9 step-10 step-11

all: $(GRAPHS)

$(GRAPHS):spawing-process/%.svg:spawing-process/%.dot
	dot -Tsvg spawing-process/$@.dot -o spawing-process/$@.svg
