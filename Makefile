.PHONY: %

PROJECT_ROOT := $(CURDIR)
PIPELINE_DIR := $(CURDIR)/scripts/update_scripts
GLOBAL_SCRIPTS := $(CURDIR)/scripts/global_scripts

%:
	@$(MAKE) -f scripts/update_scripts/Makefile $@ \
		PROJECT_ROOT="$(PROJECT_ROOT)" \
		PIPELINE_DIR="$(PIPELINE_DIR)" \
		GLOBAL_SCRIPTS="$(GLOBAL_SCRIPTS)"
