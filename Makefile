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
	scaladoc -sourcepath src -d build src/*.scala \
	  -doctitle SAWS \
	  -footer '<a href="http://zentus.com/saws">SAWS homepage</a>' \
	  -header SAWS \
	  -windowtitle SAWS
	mv build/index.html build/index-javadoc.html

pushdoc: doc
	rm -f build/sawsdoc.tgz
	cd build && \
    tar cfz sawsdoc.tgz `find . -name \*.html -or -name \*.js -or -name \*.css`
	cat build/sawsdoc.tgz | ssh hcoop "cd wixi/zentus.web/saws; tar xzfv -"

clean:
	rm -rf build
