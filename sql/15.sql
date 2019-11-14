-- using 1569245895 as a seed to the RNG


select
	s_suppkey,
	s_name,
	s_address,
	s_phone
from
	supplier,
	lineitem
where
	s_suppkey = l_suppkey
	and l_shipdate >= date '1997-09-01'
	and l_shipdate < date '1997-09-01' + interval '3' month;