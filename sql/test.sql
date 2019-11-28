select * from test_table,test_table1 where test_table.start=test_table1.start
and test_table.target=test_table1.target and test_table.start>2 and test_table1.target<3