package com.itcast.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.thrift.generated.Hbase.createTable_args;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {
	/**
	 * 配置
	 */
	static Configuration config = null;
	static {
		config = HBaseConfiguration.create();//配置
		config.set("hbase.zookeeper.quorum", "hadoop1slave1,hadoop1slave2,hadoop1slave3");//zookeeper地址
		config.set("hbase.zookeeper.property.clientPort", "2181");//zookeeper端口
	}
	public static void main(String[] args) {
		HbaseTest ht = new HbaseTest();
	}
	/**
	 * 创建一个表
	 * @param tableName：表名
	 * @param familys：列族
	 */
	public void createTable(String tableName, String[] familys) {
		try {
			HBaseAdmin admin = new HBaseAdmin(config);//hbase表管理
			if (admin.tableExists(tableName)) {//表是否存在
				System.out.println(tableName + "表已经存在!");
			} else {
				HTableDescriptor desc = new HTableDescriptor(tableName);//表的schema
				for (int i = 0; i < familys.length; i++) {
					HColumnDescriptor family = new HColumnDescriptor(familys[i]);//设计列族
					desc.addFamily(family);
				}
				admin.createTable(desc);//创建表
				System.out.println("创建表 \'" + tableName + "\' 成功!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 删除表
	 * @param tableName
	 */
	public void deleteTable(String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			if (!admin.tableExists(tableName)) {//表不存在
				System.out.println(tableName + " is not exists!");
			} else {
				admin.disableTable(tableName);//废弃表
				admin.deleteTable(tableName);//删除表
				System.out.println(tableName + " is delete!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 插入数据
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public void insertData(String tableName, String rowKey, String family,
			String qualifier, String value) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);//创建池
			table = pool.getTable(tableName);//获取表
			Put put = new Put(Bytes.toBytes(rowKey));//获取put，用于插入
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),Bytes.toBytes(value));//封装信息
			table.put(put);//添加记录
			/*
			 *批量插入
				List<Put> list = new ArrayList<Put>();
				Put put = new Put(Bytes.toBytes(rowKey));//获取put，用于插入
				put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),Bytes.toBytes(value));//封装信息
				list.add(put);
				table.put(list);//添加记录
			 * */
			System.out.println("insert a data successful!");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();//关闭表
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 删除数据
	 * @param tableName
	 * @param rowKey
	 */
	public void deleteData(String tableName, String rowKey) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);
			table = pool.getTable(tableName);
			Delete del = new Delete(Bytes.toBytes(rowKey));//创建delete
			table.delete(del);//删除
			System.out.println("delete a data successful");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 通过rewKey查询
	 * @param tableName
	 * @param rowKey
	 */
	public void queryByRowKey(String tableName, String rowKey) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);
			table = pool.getTable(tableName);
			Get get = new Get(rowKey.getBytes());//创建get
			Result row = table.get(get);//获取航记录
			//row.getValue(family, qualifier);//分别获取cell信息
			for (KeyValue kv : row.raw()) {//循环列信息
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " = ");
				System.out.print(new String(kv.getValue()));
				System.out.print(" timestamp = " + kv.getTimestamp() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 查询表的所有数据
	 * 即scan tablename
	 * @param tableName
	 */
	public void queryAll(String tableName) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);
			table = pool.getTable(tableName);
			Scan scan = new Scan();//创建scan
			ResultScanner scanner = table.getScanner(scan);//查询，返回结果
			for (Result row : scanner) {//循环scan，得到每行记录
				//row.getValue(family, qualifier);//分别获取cell信息
				System.out.println("\nRowkey: " + new String(row.getRow()));
				for (KeyValue kv : row.raw()) {//循环每列数据
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " = ");
					System.out.print(new String(kv.getValue()));
					System.out.print(" timestamp = " + kv.getTimestamp() + "\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 过滤器查询
	 * 过滤列植
	 * @param tableName
	 * @param arr
	 */
	public static void selectByFilter(String tableName, List<String> arr) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);
			table = pool.getTable(tableName);
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);// 各个条件之间是“或”的关系，默认是“与”
			Scan s1 = new Scan();
			for (String v : arr) { 
				String[] s = v.split(",");
				//列植过滤器
				filterList.addFilter(new SingleColumnValueFilter(Bytes
						.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL,
						Bytes.toBytes(s[2])));
				// 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回
				// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
			}
			s1.setFilter(filterList);
			ResultScanner rs = table.getScanner(s1);
			for (Result rr = rs.next(); rr != null; rr = rs
					.next()) {
				for (KeyValue kv : rr.list()) {
					System.out.println("row : " + new String(kv.getRow()));
					System.out.println("column : " + new String(kv.getFamily())
							+ ":" + new String(kv.getQualifier()));
					System.out.println("value : " + new String(kv.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
