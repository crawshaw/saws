ifdef RELEASE
sflags := -deprecation -optimise
else
sflags := -deprecation
endif

version := 0.1

jar: build/aws-$(version).jar

build/aws-$(version).jar: class
	@rm -f $@
	jar cf $@ -C build com

class: $(wildcard src/*.scala)
	@mkdir -p build
	fsc $(sflags) -sourcepath src -d build src/*.scala

doc:
	scaladoc -sourcepath src -d build src/*.scala

clean:
	rm -rf build
