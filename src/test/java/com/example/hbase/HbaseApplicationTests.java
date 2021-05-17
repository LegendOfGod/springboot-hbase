package com.example.hbase;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;


@SpringBootTest
class HbaseApplicationTests {

    @Autowired
    private Connection connection;

    @Test
    void testCreateTable() throws IOException {
        TableName myTable = TableName.valueOf("myTable1");
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(myTable);
        List<ColumnFamilyDescriptor> columnFamilies = new ArrayList<>();
        columnFamilies.add(ColumnFamilyDescriptorBuilder.newBuilder("cf1".getBytes()).setTimeToLive(10).build());
        columnFamilies.add(ColumnFamilyDescriptorBuilder.newBuilder("cf2".getBytes()).setTimeToLive(10).build());
        tableDescriptorBuilder.setColumnFamilies(columnFamilies);
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        connection.getAdmin().createTable(tableDescriptor);
    }

    @Test
    void testInsertTestTable() throws IOException {
        TableName myTable = TableName.valueOf("mytable");
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(myTable);
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder("mycf".getBytes()).build();
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        connection.getAdmin().createTable(tableDescriptor);
    }

    @Test
    void testInsertTestData() throws IOException {
        TableName myTable = TableName.valueOf("mytable");
        Table table = connection.getTable(myTable);
        List<Put> puts = new ArrayList<>();
        Put put = new Put("row1".getBytes());
        put.addColumn("mycf".getBytes(), "name".getBytes(), "billyWangpaul".getBytes());
        puts.add(put);

        Put put1 = new Put("row2".getBytes());
        put1.addColumn("mycf".getBytes(), "name".getBytes(), "sara".getBytes());
        puts.add(put1);

        Put put2 = new Put("row3".getBytes());
        put2.addColumn("mycf".getBytes(), "name".getBytes(), "chris".getBytes());
        puts.add(put2);

        Put put3 = new Put("row4".getBytes());
        put3.addColumn("mycf".getBytes(), "name".getBytes(), "helen".getBytes());
        puts.add(put3);

        Put put4 = new Put("row5".getBytes());
        put4.addColumn("mycf".getBytes(), "name".getBytes(), "andyWang".getBytes());
        puts.add(put4);

        Put put5 = new Put("row6".getBytes());
        put5.addColumn("mycf".getBytes(), "name".getBytes(), "kateWang".getBytes());
        puts.add(put5);

        table.put(puts);
    }

    @Test
    void testInsertOrUpdate() throws IOException {
        //先获取Table对象
        String tableName = "myTable";
        Table table = connection.getTable(TableName.valueOf(tableName));
        //构造put对象
        String rowkey = "row1";
        Put put = new Put(rowkey.getBytes());
        put.addColumn(Bytes.toBytes("cf1"), null, Bytes.toBytes("张无忌"));
        put.addColumn(Bytes.toBytes("cf1"), null, Bytes.toBytes("男"));
        put.addColumn(Bytes.toBytes("cf2"), null, Bytes.toBytes("赵敏"));
        put.addColumn(Bytes.toBytes("cf2"), null, Bytes.toBytes("张翠山"));
        put.addColumn(Bytes.toBytes("cf2"), null, Bytes.toBytes("殷素素"));
        table.put(put);
    }

    @Test
    void testDeleteRow() throws IOException {
        //先获取Table对象
        String tableName = "myTable";
        Table table = connection.getTable(TableName.valueOf(tableName));
        //构造delete对象
        String rowkey = "row1";
        Delete delete = new Delete(rowkey.getBytes());
        table.delete(delete);
    }

    @Test
    void testDeleteColumnFamily() throws IOException {
        //先获取Table对象
        String tableName = "myTable";
        Table table = connection.getTable(TableName.valueOf(tableName));
        //构造delete对象
        String rowkey = "row1";
        Delete delete = new Delete(rowkey.getBytes());
        delete.addFamily(Bytes.toBytes("cf2"));
        table.delete(delete);
    }

    @Test
    void testDeleteColumn() throws IOException {
        //先获取Table对象
        String tableName = "myTable";
        Table table = connection.getTable(TableName.valueOf(tableName));
        //构造delete对象
        String rowkey = "row1";
        Delete delete = new Delete(rowkey.getBytes());
        delete.addColumn("cf2".getBytes(), "mam".getBytes());
        table.delete(delete);
    }

    @Test
    void testExistTable() throws IOException {
        //先获取Table对象
        String tableName = "myTable";
        Table table = connection.getTable(TableName.valueOf(tableName));
        boolean tableExists = connection.getAdmin().tableExists(TableName.valueOf(tableName));
        System.out.println(tableExists);

    }

    @Test
    void testDeleteTable() throws IOException {
        //先获取Table对象
        String tableName = "myTable1";
        TableName tableName1 = TableName.valueOf(tableName);
        boolean tableExists = connection.getAdmin().tableExists(tableName1);
        System.out.println(tableExists);
        if (tableExists) {
            connection.getAdmin().disableTable(tableName1);
            connection.getAdmin().deleteTable(tableName1);
        }
    }

    @Test
    void testGetValue() throws IOException {
        //先获取Table对象
        TableName tableName = TableName.valueOf("myTable");
        Table table = connection.getTable(tableName);
        Get get = new Get("row1".getBytes());
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        if (!CollectionUtils.isEmpty(cells)) {
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
        }
        table.close();
        connection.close();
    }

    @Test
    void testSelectOneRow() throws IOException {
        TableName tableName = TableName.valueOf("myTable");
        Table table = connection.getTable(tableName);
        Get get = new Get("row1".getBytes());
        Result result = table.get(get);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
            String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println(row);
            System.out.println(columnFamily);
            System.out.println(column);
            System.out.println(value);

        }
    }

    @Test
    void testScanTable() throws IOException {
        TableName tableName = TableName.valueOf("myTable");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println("------->" + cell);
            }
        }
        scanner.close();
    }

    /**
     * 值过滤器筛选单位是cell 符合的列会被过滤出来  但是改行的其他不符的是不会放到结果集中的
     *
     * @throws IOException
     */
    @Test
    void testValueFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf("myTable"));
        ValueFilter wang = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("张无"));
        Scan scan = new Scan();
        scan.setFilter(wang);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println("----------------------------");
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }

        }
        scanner.close();
    }

    /**
     * 单列值过滤器指定列名进行过滤 结果集里面包含：1、符合该过滤器条件的此行数据包括其他列值 2、如果某行数据没有这个列也会被整行加入结果集当中
     *
     * @throws IOException
     */
    @Test
    void testSingleColumnValueFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf("myTable"));
        SingleColumnValueFilter wang = new SingleColumnValueFilter("cf1".getBytes(), "sex".getBytes(), CompareOperator.EQUAL, new SubstringComparator("男"));
        Scan scan = new Scan();
        scan.setFilter(wang);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }

        }
        scanner.close();
    }

    /**
     * 列簇过滤器 返回的结果集是所有行下此列簇下所有的列值
     *
     * @throws IOException
     */
    @Test
    void testFamilyFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf("myTable"));
        FamilyFilter familyFilter = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator("cf2".getBytes()));
        Scan scan = new Scan();
        scan.setFilter(familyFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }

        }
        scanner.close();
    }


    /**
     * 列过滤器 返回的结果集只包含指定给的列名
     *
     * @throws IOException
     */
    @Test
    void testColumnFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf("myTable"));
        QualifierFilter qualifierFilter = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator("name".getBytes()));
        Scan scan = new Scan();
        scan.setFilter(qualifierFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }

        }
        scanner.close();
    }

    /**
     * 过滤器列表 用于多条件组合查询
     *
     * @throws IOException
     */
    @Test
    void testFilterList() throws IOException {
        Table table = connection.getTable(TableName.valueOf("myTable"));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        FamilyFilter familyFilter = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("cf1")));
        filterList.addFilter(familyFilter);
        QualifierFilter qualifierFilter = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator("name".getBytes()));
        filterList.addFilter(qualifierFilter);
        ValueFilter valueFilter = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("张"));
        filterList.addFilter(valueFilter);
        Scan scan = new Scan();
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }
        }
        scanner.close();
    }


    /**
     * 比较器=
     *
     * @throws IOException
     */
    @Test
    void testEqualValueFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf("myTable"));
        ValueFilter valueFilter = new ValueFilter(CompareOperator.EQUAL, new BinaryComparator("张无忌".getBytes()));
        Scan scan = new Scan();
        scan.setFilter(valueFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }
        }
        scanner.close();
    }


    /**
     * 比较器<
     *
     * @throws IOException
     */
    @Test
    void testGreaterValueFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf("myTable"));
        ValueFilter valueFilter = new ValueFilter(CompareOperator.GREATER, new BinaryComparator("张".getBytes()));
        Scan scan = new Scan();
        scan.setFilter(valueFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }
        }
        scanner.close();
    }

    /**
     * put数据
     *
     * @throws IOException
     */
    @Test
    void testInsertNum() throws IOException {
        TableName tableName = TableName.valueOf("myage");
       /* TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table);
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder("mycf".getBytes()).build();
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        connection.getAdmin().createTable(tableDescriptorBuilder.build());*/
        Table table = connection.getTable(tableName);
        List<Put> puts = new ArrayList<>();
        Put put = new Put("row1".getBytes());
        put.addColumn("mycf".getBytes(), "address".getBytes(), Bytes.toBytes("beijing"));
        puts.add(put);

        Put put1 = new Put("row2".getBytes());
        put1.addColumn("mycf".getBytes(), "address".getBytes(), Bytes.toBytes("beijing"));
        puts.add(put1);

        Put put2 = new Put("row3".getBytes());
        put2.addColumn("mycf".getBytes(), "address".getBytes(), Bytes.toBytes("hangzhou"));
        puts.add(put2);

        Put put3 = new Put("row4".getBytes());
        put3.addColumn("mycf".getBytes(), "address".getBytes(), Bytes.toBytes("hangzhou"));
        puts.add(put3);

        Put put4 = new Put("row5".getBytes());
        put4.addColumn("mycf".getBytes(), "address".getBytes(), Bytes.toBytes("shanghai"));
        puts.add(put4);

        Put put5 = new Put("row6".getBytes());
        put5.addColumn("mycf".getBytes(), "address".getBytes(), Bytes.toBytes("shanghai"));
        puts.add(put5);
        table.put(puts);
    }

    @Test
    void testGreaterNum() throws IOException {
        TableName tableName = TableName.valueOf("myage");
        Table table = connection.getTable(tableName);
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("mycf".getBytes(), "age".getBytes(), CompareOperator.GREATER,
                new BinaryComparator(Bytes.toBytes(10)));
        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }
        }
    }

    /**
     * 嵌套查询是（A OR B） AND C
     *
     * @throws IOException
     */
    @Test
    void testNestedQueries() throws IOException {
        //查询条件是（address = 'beijing' or address = 'hangzhou'） and name = 'zhangsan';
        TableName tableName = TableName.valueOf("myage");
        Table table = connection.getTable(tableName);

        //构造内层filterlist
        List<Filter> innerFilters = new ArrayList<>();
        SingleColumnValueFilter beijingFilter = new SingleColumnValueFilter("mycf".getBytes(), "address".getBytes(),
                CompareOperator.EQUAL, new BinaryComparator("beijing".getBytes()));
        innerFilters.add(beijingFilter);
        SingleColumnValueFilter shanghaiFilter = new SingleColumnValueFilter("mycf".getBytes(), "address".getBytes(),
                CompareOperator.EQUAL, new BinaryComparator("shanghai".getBytes()));
        innerFilters.add(shanghaiFilter);
        FilterList innerfilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, innerFilters);
        //构造外层filter
        List<Filter> outerFilters = new ArrayList<>();
        outerFilters.add(innerfilterList);
        SingleColumnValueFilter zhangsanFilter = new SingleColumnValueFilter("mycf".getBytes(), "name".getBytes(),
                CompareOperator.EQUAL, new BinaryComparator("zhangsan".getBytes()));
        outerFilters.add(zhangsanFilter);
        FilterList outerfilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, outerFilters);
        Scan scan = new Scan();
        scan.setFilter(outerfilterList);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            String row = Bytes.toString(result.getRow());
            String name = Bytes.toString(result.getValue("mycf".getBytes(), "name".getBytes()));
            System.out.println(row + ":" + name);
        }
    }


    /**
     * 行过滤器  返回的结果集中包含了符合的行键的所有列值
     *
     * @throws IOException
     */
    @Test
    void testRowFilter() throws IOException {
        TableName tableName = TableName.valueOf("myTable");
        Table table = connection.getTable(tableName);

        RowFilter rowFilter = new RowFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator("row3".getBytes()));
        Scan scan = new Scan();
        scan.setFilter(rowFilter);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }
        }
    }


    /**
     * 行键范围过滤器
     *
     * @throws IOException
     */
    @Test
    void testRowRangeFilter() throws IOException {
        TableName tableName = TableName.valueOf("myTable");
        Table table = connection.getTable(tableName);

        MultiRowRangeFilter.RowRange rowRange1to2 = new MultiRowRangeFilter.RowRange("row1", true,
                "row2", true);
        MultiRowRangeFilter.RowRange rowRange5to7 = new MultiRowRangeFilter.RowRange("row3", true,
                "row7", true);
        List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        rowRanges.add(rowRange5to7);
        rowRanges.add(rowRange1to2);
        MultiRowRangeFilter rowRangeFilter = new MultiRowRangeFilter(rowRanges);
        Scan scan = new Scan();
        scan.setFilter(rowRangeFilter);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }
        }
    }

    /**
     * 行键前缀过滤器
     *
     * @throws IOException
     */
    @Test
    void testPrefixRowFilter() throws IOException {
        TableName tableName = TableName.valueOf("myTable");
        Table table = connection.getTable(tableName);
        PrefixFilter prefixFilter = new PrefixFilter("row1".getBytes());
        Scan scan = new Scan();
        scan.setFilter(prefixFilter);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }
        }
    }

    /**
     * 模糊行键过滤器 可以用?来标识需要匹配的字段 需要掩码1来制定需要模糊匹配的字符位置 和 0来指定不需要模糊匹配的位置
     *
     * @throws IOException
     */
    @Test
    void testFuzzyRowFilter() throws IOException {
        TableName tableName = TableName.valueOf("myTable");
        Table table = connection.getTable(tableName);
        Pair<byte[], byte[]> pair = new Pair<>(Bytes.toBytes("?o?2"), new byte[]{1, 0, 1, 0});
        FuzzyRowFilter fuzzyRowFilter = new FuzzyRowFilter(Collections.singletonList(pair));
        Scan scan = new Scan();
        scan.setFilter(fuzzyRowFilter);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }
        }
    }


    /**
     * 列前缀过滤器 返回的结果集只有符合前缀的列 该行的其他列并不会被加入到结果集当中
     *
     * @throws IOException
     */
    @Test
    void testColumnPrefixFilter() throws IOException {
        TableName tableName = TableName.valueOf("myTable");
        Table table = connection.getTable(tableName);
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter("na".getBytes());
        Scan scan = new Scan();
        scan.setFilter(columnPrefixFilter);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }
        }
    }

    /**
     * 组合列前缀过滤器 是或的关系  任意满足一个前缀条件都会被加入结果集当中 bytes二维数组的长度必须过滤前缀的数量一致否则会抛异常
     *
     * @throws IOException
     */
    @Test
    void testMultiColumnPrefixFilter() throws IOException {
        TableName tableName = TableName.valueOf("myTable");
        Table table = connection.getTable(tableName);
        byte[][] filter_prefix = new byte[3][];
        filter_prefix[0] = Bytes.toBytes("na");
        filter_prefix[1] = Bytes.toBytes("se");
        filter_prefix[2] = Bytes.toBytes("da");
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(filter_prefix);
        Scan scan = new Scan();
        scan.setFilter(multipleColumnPrefixFilter);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }
        }
    }

    /**
     * keyOnlyFilter是不会包含列值的 FirstKeyOnlyFilter是包含第一个列值的 从计数的角度来说FirstKeyOnlyFilter效率更高
     *
     * @throws IOException
     */
    @Test
    void testKeyOnlyFilter() throws IOException {
        TableName tableName = TableName.valueOf("myTable");
        Table table = connection.getTable(tableName);
        //KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
        FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter();
        Scan scan = new Scan();
        scan.setFilter(firstKeyOnlyFilter);
        ResultScanner results = table.getScanner(scan);
        int count = 0;
        for (Result result : results) {
            count++;
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println("column=" + new String(CellUtil.cloneQualifier(cell)));
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println(row + "-->" + columnFamily + ":" + column + "-->" + value);
            }
        }
        System.out.println("count=" + count);
    }

    @Test
    void testFirstKeyOnlyFilter() throws IOException {
        TableName tableName = TableName.valueOf("myage");
        Table table = connection.getTable(tableName);
        FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter();
        Scan scan = new Scan();
        scan.setFilter(firstKeyOnlyFilter);
        ResultScanner results = table.getScanner(scan);
        int count = 0;
        for (Result result : results) {
            count++;
        }
        System.out.println(count);
    }

    /**
     * 指定行 列过滤 只返回指定的前n列属性
     * @throws IOException
     */
    @Test
    void testColumnCountGetFilter() throws IOException {
        TableName tableName = TableName.valueOf("myTable");
        Table table = connection.getTable(tableName);
        Get get = new Get("row1".getBytes());
        ColumnCountGetFilter columnCountGetFilter = new ColumnCountGetFilter(3);
        get.setFilter(columnCountGetFilter);
        Result result = table.get(get);
        System.out.println(Bytes.toString(result.getRow()));
        System.out.println(result);
    }

    @Test
    void testAdmin() throws Throwable {
      /*  System.out.println(clusterMetrics.getHBaseVersion());
        System.out.println(clusterMetrics.getMasterName());
        int count = clusterMetrics.getRegionCount();
        System.out.println(count);
        System.out.println(clusterMetrics.getRequestCount());
        System.out.println(clusterMetrics.getLiveServerMetrics());
        System.out.println(clusterMetrics.getDeadServerNames());
        Collection<ServerName> regionServers = connection.getAdmin().getRegionServers();
        System.out.println(regionServers);
        double averageLoad = clusterMetrics.getAverageLoad();
        System.out.println(averageLoad);*/
        VisibilityLabelsProtos.ListLabelsResponse labelsResponse = VisibilityClient.listLabels(connection, ".*");
        List<ByteString> labelList = labelsResponse.getLabelList();
        for (ByteString bytes : labelList) {
            System.out.println(bytes.toStringUtf8());
        }
    }
}
