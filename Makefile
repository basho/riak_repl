.PHONY: rel deps test

all: deps compile

compile:
	@./rebar compile

deps:
	@./rebar get-deps

clean:
	@./rebar clean

distclean: clean
	@./rebar delete-deps

test: all
	@./rebar skip_deps=true eunit

xref:
	@echo "Xref not supported for 1.4 branch. Sorry buildbot. (skipping)"

dialyzer:
	@echo "Dialyzer not supported for 1.4 branch. Sorry buildbot. (skipping)"
