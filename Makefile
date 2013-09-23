eunit:
	../../rebar eunit deps_dir=.. skip_deps=true

eqc_dev:
	./rebar eunit skip_deps=true compile_only=true
