
version := 0.1

jar: build/aws-$(version).jar

build/aws-$(version).jar: class
	@rm -f $@
	jar cf $@ -C build com

class: $(wildcard src/*.scala)
	@mkdir -p build
	fsc -optimise -deprecation -sourcepath src -d build src/*.scala

clean:
	rm -rf build
