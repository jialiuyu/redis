
==========create ubmem==========

1 ubmem provider
# reserve hugetlb 128GB per numa
echo 65536 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages
echo 65536 > /sys/devices/system/node/node1/hugepages/hugepages-2048kB/nr_hugepages

# export ubmem
obmmctl export --source numa=0,size=8G --allow_mmap true --deid_l `cat /sys/devices/ub_bus_controller0/00001/eid
## output demo
{
	id:2, // this is used at ubmem provider when obmm shmdev open and mmap
	uba: 0xfffe00000000, // this is used at ubmem user when ubmem import
	size: 0x200000000,
	tokenid: 1 // this is used at ubmem user when ubmem import
}

# ubmem provider mmap the export mem, see "https://atomgit.com/openeuler/obmm/blob/master/doc/obmm_shmdev.md"


2 ubmem user
# query local ub config
cat /proc/mami/udm/udevice
## ouput demo
udevid[30] cna[81] eid[262225] location[0] chip_type[1650_1P]
            guid[guid_8000:4604:b700:0000:0000:0002:00a0:08cc]

# config decoder, allocate HPA addr
./drv_mami_dt ubmem decoder_single_cfg devid 30  chipId 0 dieId 0 dstCNA 17 importType 0 portSetId 0 decoderIdx 0 lb 0 tokenId 1 flag 0x60 uba 0xfffe00000000 size 0x200000000 handle 0
## devid is from local udevid
## dstCNA is from ubmem provider's udevid (use cat /proc/mami/udm/udevice in ubmem provider)
## tokenId, uba and size are from output of "obmmctl export"
## other params are fixed as below.

# import ubmem
obmmctl import --addr 0x20000000000 --size 8G --scna 81 --allow_mmap true --seid_l 262225
## --addr is from step "config decoder, allocate HPA addr"
## --size is from ubmem provider step "export ubmem"
## --scna and --seid_l are from step "query local ub config"
## output: mem_id, used when mmap and unimport

# ubmem user mmap the import mem, see "https://atomgit.com/openeuler/obmm/blob/master/doc/obmm_shmdev.md"
## if mmap NC: see "UB Memory share config.pptx"
	fd = open("/dev/obmm_shmdev2", O_RDWR|O_SYNC);
	ubmem_ptr = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);


==========delete ubmem==========
3 ubmem user
# obmm_shmdev* clean up
munmap, close(fd)

# unimport
obmmctl unimport --mem_id <mem_id from import>

# clean decoder
./drv_mami_dt ubmem handle_query devid 30 chipId 0 dieId 0 portSetId 0 decoderIdx 0 queryNum 10 type 4
./drv_mami_dt ubmem decoder_del devid 30 chipId 0 dieId 0 portSetId 0 decoderIdx 0 handle <handle from query>


4 ubmem provider
# obmm_shmdev* clean up
munmap, close(fd)

# unexport
obmmctl unexport --mem_id <mem_id from export>