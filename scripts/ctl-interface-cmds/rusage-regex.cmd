# Print selected 'system_info'
GET /system_info/current_time\|rusage\.\(max_rss\|maj_fault\|.*block\|vol\)
OUTFILE /rusage.out
#
#{
#        "system_info" : {
#                "current_time" : "Wed Apr 29 12:45:27 2020",
#                "rusage.max_rss" : 401552,
#                "rusage.maj_fault" : 0,
#                "rusage.in_block" : 24,
#                "rusage.out_block" : 3560,
#                "rusage.vol_ctsw" : 5836
#        }
#}
