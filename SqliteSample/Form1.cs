using System;
using System.IO;
using System.Windows.Forms;
using System.Data.SQLite;
using System.Data;
using System.Diagnostics;

namespace SqliteSample
{
	public partial class Form1 : Form
	{
		public Form1()
		{
			InitializeComponent();
		}

		private void Form1_Load(object sender, EventArgs e)
		{

		}

		private void button1_Click(object sender, EventArgs e)
		{
			string currentPath = Path.GetDirectoryName(typeof(Program).Assembly.Location);
			string sqlFilePath = Path.Combine(currentPath, "barcodedb.sqlite");

			string connectionString = $"Data Source={sqlFilePath};";

			using (SQLiteConnection connection = new SQLiteConnection(connectionString))
			{
				connection.Open();

				CreateTableIfNotExists(connection);

				//VACUUM;
				SQLiteCommand vacuumCommand = new SQLiteCommand();
				vacuumCommand.Connection = connection;
				vacuumCommand.CommandText = "VACUUM;";
				vacuumCommand.ExecuteNonQuery();

				SQLiteCommand insertCommand = new SQLiteCommand();
				insertCommand.Connection = connection;
				insertCommand.CommandText = "INSERT INTO mytable(id, NAME, age, DESCRIPTION) VALUES (@id, @NAME, @age, @DESCRIPTION)";

				insertCommand.Parameters.Add("@id", DbType.String);
				insertCommand.Parameters.Add("@NAME", DbType.String, 50);
				insertCommand.Parameters.Add("@age", DbType.Int32);
				insertCommand.Parameters.Add("@DESCRIPTION", DbType.String, 150);

				string nameValue = "Name" + Guid.NewGuid().ToString();
				insertCommand.Parameters[0].Value = (int)DateTime.Now.Ticks;
				insertCommand.Parameters[1].Value = nameValue;
				insertCommand.Parameters[2].Value = 10;
				insertCommand.Parameters[3].Value = nameValue + "_Description";

				int affected = insertCommand.ExecuteNonQuery();
				Console.WriteLine("# of affected row: " + affected);

				// Select - ExecuteScalar
				Stopwatch st3 = new Stopwatch();
				st3.Start();
				SQLiteCommand selectCommand = new SQLiteCommand();
				selectCommand.Connection = connection;
				selectCommand.CommandText = "SELECT * FROM mytable";

				SQLiteDataReader reader = selectCommand.ExecuteReader();
				DataTable dt = new DataTable();
				dt.Load(reader);

				Console.WriteLine(dt.Rows.Count);

				st3.Stop();
				Console.WriteLine(st3.Elapsed);

				//object result = selectCommand.ExecuteScalar();
				//Console.WriteLine("# of records: " + result);


				//대량 데이터
				Stopwatch st2 = new Stopwatch();
				st2.Start();
				using (SQLiteTransaction mytransaction = connection.BeginTransaction())
				{
					insertCommand.Transaction = mytransaction;
					insertCommand.Connection = connection;

					for (int i = 0; i < 100; i++)
					{
						string nameValue1 = "Name" + Guid.NewGuid().ToString();
						insertCommand.Parameters[0].Value = (int)DateTime.Now.Ticks;
						insertCommand.Parameters[1].Value = nameValue1;
						insertCommand.Parameters[2].Value = 10;
						insertCommand.Parameters[3].Value = nameValue1 + "_Description";
						insertCommand.Prepare();
						int affected1 = insertCommand.ExecuteNonQuery();
						//Console.WriteLine("# of affected row: " + affected1);
					}

					mytransaction.Commit();
				}
				st2.Stop();
				Console.WriteLine(st2.Elapsed);


				using (SqliteBulk bulk = new SqliteBulk(connection))
				{
					for (int i = 0; i < 100; i++)
					{
						nameValue = "Name" + Guid.NewGuid().ToString();
						insertCommand.Parameters[0].Value = (int)DateTime.Now.Ticks;
						insertCommand.Parameters[1].Value = nameValue;
						insertCommand.Parameters[2].Value = 10;
						insertCommand.Parameters[3].Value = nameValue + "_Description";

						int affected2 = insertCommand.ExecuteNonQuery();
						Console.WriteLine("# of affected row: " + affected2);
					}
				}



				Stopwatch st = new Stopwatch();
				st.Start();
				SQLiteCommand insertCommand2 = new SQLiteCommand();
				insertCommand2.Connection = connection;
				insertCommand2.CommandText = "INSERT INTO items(item_id, item_nm) VALUES (@item_id, @item_nm)";
				insertCommand2.Parameters.Add("@item_id", DbType.String);
				insertCommand2.Parameters.Add("@item_nm", DbType.String);

				using (SQLiteTransaction mytransaction = connection.BeginTransaction())
				{
					insertCommand2.Transaction = mytransaction;
					insertCommand2.Connection = connection;

					for (int i = 0; i < 1000; i++)
					{
						//insertCommand2.Parameters.AddWithValue("@item_id", i.ToString());
						//insertCommand2.Parameters.AddWithValue("@item_nm", i.ToString());
						insertCommand2.Parameters[0].Value = i.ToString();
						insertCommand2.Parameters[1].Value = i.ToString();
						insertCommand2.Prepare();

						int affected1 = insertCommand2.ExecuteNonQuery();
						//Console.WriteLine("# of affected row: " + affected1);
					}

					mytransaction.Commit();
				}
				st.Stop();
				Console.WriteLine(st.Elapsed);



				// Update
				SQLiteCommand updateCommand = new SQLiteCommand();
				updateCommand.Connection = connection;
				updateCommand.CommandText = "UPDATE mytable SET DESCRIPTION=@DESCRIPTION WHERE NAME=@NAME";

				updateCommand.Parameters.Add("@NAME", DbType.String);
				updateCommand.Parameters.Add("@DESCRIPTION", DbType.String);

				updateCommand.Parameters[0].Value = nameValue;
				updateCommand.Parameters[1].Value = nameValue + "_Description2";

				affected = updateCommand.ExecuteNonQuery();
				Console.WriteLine("# of affected row: " + affected);


				// Delete
				SQLiteCommand deleteCommand = new SQLiteCommand();
				deleteCommand.Connection = connection;
				deleteCommand.CommandText = "DELETE FROM mytable WHERE NAME=@NAME";

				deleteCommand.Parameters.Add("@NAME", DbType.String);
				deleteCommand.Parameters[0].Value = nameValue;

				affected = deleteCommand.ExecuteNonQuery();
				Console.WriteLine("# of affected row: " + affected);


				// Select - ExecuteScalar
				SQLiteCommand selectCommand2 = new SQLiteCommand();
				selectCommand2.Connection = connection;
				selectCommand2.CommandText = "SELECT count(*) FROM mytable";


				object result = selectCommand2.ExecuteScalar();
				Console.WriteLine("# of records: " + result);
			}
		}
		private static void CreateTableIfNotExists(SQLiteConnection conn)
		{
			string sql = "create table if not exists mytable(id int, NAME varchar(50), age int, DESCRIPTION varchar(150))";
			new SQLiteCommand(sql, conn).ExecuteNonQuery();

			sql = "create index if not exists idx_NAME on mytable(NAME)";
			new SQLiteCommand(sql, conn).ExecuteNonQuery();


			sql = @"
					CREATE TABLE if not exists invoices
					(
						[invoice_id] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
						[invoice_date] TEXT NOT NULL,
						[invoice_title] TEXT NOT NULL
					)
				   ";
			new SQLiteCommand(sql, conn).ExecuteNonQuery();

			sql = @"
					CREATE INDEX if not exists [IDX_InvoiceInvoiceDate] ON invoices ([invoice_date])
				   ";
			new SQLiteCommand(sql, conn).ExecuteNonQuery();


			sql = @"
					CREATE TABLE if not exists invoice_items
					(
						[invoice_line_id] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
						[invoice_id] INTEGER  NOT NULL,
						[item_id] TEXT  NOT NULL,
						[item_nm] TEXT NOT NULL,
						[order_qty] INTEGER  NOT NULL DEFAULT 0,
						[scan_qty] INTEGER  NOT NULL DEFAULT 0,
						FOREIGN KEY ([invoice_id]) REFERENCES invoices ([invoice_id]) 
							ON DELETE NO ACTION ON UPDATE NO ACTION
					)
				   ";
			new SQLiteCommand(sql, conn).ExecuteNonQuery();

			sql = @"
					CREATE INDEX if not exists [IDX_invoice_items1] ON invoice_items ([invoice_id], [item_id])
				   ";
			new SQLiteCommand(sql, conn).ExecuteNonQuery();

			sql = @"
					CREATE TABLE if not exists items
					(
						[item_id] TEXT PRIMARY KEY NOT NULL,
						[item_nm] TEXT NOT NULL
					)
				   ";
			new SQLiteCommand(sql, conn).ExecuteNonQuery();

		}


	}

	public class SqliteBulk : IDisposable
	{
		SQLiteConnection _connection;

		public SqliteBulk(SQLiteConnection connection)
		{
			_connection = connection;

			SQLiteCommand begin = new SQLiteCommand("begin", connection);
			begin.ExecuteNonQuery();
		}

		public void Dispose()
		{
			SQLiteCommand begin = new SQLiteCommand("end", _connection);
			begin.ExecuteNonQuery();
		}
	}
}
