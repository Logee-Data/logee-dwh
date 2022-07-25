from data_cataloger.cataloger import Table

def test_Table_general():
	table: Table = Table(
		table_id='test_floor.test_floor.test_subfloor.test_table',
        floor='test_floor',
        subfloor='test_subfloor',
        name='test_table',
        columns_defined_list=[],
        description='test table description',
	)

	assert isinstance(table.columns_defined, dict)
	assert table.columns_inherited == None
	assert isinstance(table.is_inheritances_determined, bool)
