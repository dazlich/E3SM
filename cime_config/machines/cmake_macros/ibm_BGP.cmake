string(APPEND CFLAGS " -qtune=450 -qarch=450 -I/bgsys/drivers/ppcfloor/arch/include/")
set(CONFIG_ARGS "--build=powerpc-bgp-linux --host=powerpc64-suse-linux")
string(APPEND CPPDEFS " -DLINUX -DnoI8")
string(APPEND FFLAGS " -qspillsize=2500 -qtune=450 -qarch=450")
string(APPEND FFLAGS " -qextname=flush")
set(LDFLAGS "-Wl,--relax -Wl,--allow-multiple-definition")
