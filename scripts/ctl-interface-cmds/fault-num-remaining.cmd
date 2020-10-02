APPLY num_remaining@90
WHERE /fault_injection_points/name@raft_follower_ignores_non_hb_AE_request
OUTFILE /err.out
