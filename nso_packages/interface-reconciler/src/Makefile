all: fxs

include $(NCS_DIR)/src/ncs/build/include.ncs.mk

SRC  = $(wildcard yang/*.yang)
FXS  = $(SRC:yang/%.yang=../load-dir/%.fxs)
DIRS = ../load-dir ../python/_namespaces

fxs: $(FXS)

$(DIRS):
	mkdir -p $@

../load-dir/%.fxs: yang/%.yang $(NCSC) | $(DIRS)
	$(NCSC) $(YANGPATH) -c -o $@ $<
	$(NCSC) --emit-python ../python/_namespaces/$(notdir $(basename $<)).py $@

clean:
	rm -rf $(DIRS)
	find . -name "*.pyc" -delete

.PHONY: all fxs clean
