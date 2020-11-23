using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using System.Data.OleDb;
using System.Configuration;
using MySql.Data.MySqlClient;
using System.Threading.Tasks;
using MySql.Data;
using System.Security.Cryptography;
using System.Data;
using System.Threading;

namespace kanal
{
    class Program
    {
        static void Main(string[] args)
        {
           
            DosyalariYaz();
           
          
            Console.ReadKey();
         
           
        }

        public static void DosyalariYaz()
        {
            i:
            channelYazdir();     

            MySqlConnection conn = new MySqlConnection("server=localhost;Port=3306;User=root;database=santral;password=1234");
            conn.Open();

            DataTable dt = new DataTable();

            MySqlDataAdapter da = new MySqlDataAdapter("select * from dosyalar", conn);
            da.Fill(dt);

            string path = "C:\\genex\\db";

            DirectoryInfo di = new DirectoryInfo(path);
            FileInfo[] rgfiles = di.GetFiles();

            DateTime zaman = DateTime.Now;

           
            foreach (FileInfo d in rgfiles)
            {

                string key = d.FullName.ToString() + d.CreationTime.ToString();//isim ve oluşturulma zamanı

                System.Text.UnicodeEncoding obje = new System.Text.UnicodeEncoding();
                byte[] bytString = obje.GetBytes(key);
                MD5CryptoServiceProvider objProv = new MD5CryptoServiceProvider();
                byte[] hash = objProv.ComputeHash(bytString);
                string hashkey = Convert.ToBase64String(hash);

                if (d.Extension == ".mdb")
                {

                    MySqlConnection conn2 = new MySqlConnection("server=localhost;Port=3306;User=root;database=santral;password=1234");
                    conn2.Open();

                    string keysorgu = "select count(_key) as sayi from dosyalar where _key='" + hashkey + "'";
                    MySqlCommand keycmd = new MySqlCommand(keysorgu, conn2);
                    int sayi = Convert.ToInt32(keycmd.ExecuteScalar());

                    if (sayi < 1)
                    {
                        MySqlCommand cmd = new MySqlCommand("insert into dosyalar(name,server_client,size,_date,_datetime,relative_path,completed,is_db,created,updated,_key,_hash) values(@name,@server_client,@size,@_date,@_datetime,@relative_path,@completed,@is_db,@created,@updated,@_key,@_hash)", conn2);
                     

                        //name,server_client,size,_date,_datetime,relative_path,complated,error,is_db,created,updated,key,hash
                        cmd.Parameters.AddWithValue("@name", d.Name.ToString());
                        cmd.Parameters.AddWithValue("@server_client", d.FullName.ToString());
                        cmd.Parameters.AddWithValue("@size", d.Length);
                        cmd.Parameters.AddWithValue("@_date", d.LastAccessTime.ToString());// son erişim zamanı
                        cmd.Parameters.AddWithValue("@_datetime", zaman.ToString());
                        cmd.Parameters.AddWithValue("@relative_path", d.DirectoryName.ToString());
                        cmd.Parameters.AddWithValue("@completed", 0);
                        //cmd.Parameters.AddWithValue("@error", 0);
                        cmd.Parameters.AddWithValue("@is_db", 1);
                        cmd.Parameters.AddWithValue("@created", d.CreationTime.ToString());//oluşturulma tarihi
                        cmd.Parameters.AddWithValue("@updated", d.LastWriteTime.ToString());//son değişiklik zamanı
                        cmd.Parameters.AddWithValue("@_key", hashkey);
                        cmd.Parameters.AddWithValue("@_hash", hashkey);
                    
                                         
                        cmd.ExecuteNonQuery();
                         kayitYaz(1);
                        //Console.WriteLine( "here");
                        conn2.Close();
                    }
                      
                }              
            }

            conn.Close();
            Thread.Sleep(2000);
            goto i;

        }

        public static int kayitYaz(int deger)
        {
            MySqlConnection conn = new MySqlConnection("server=localhost;Port=3306;User=root;database=santral;password=1234");
            conn.Open();

            MySqlCommand varmi2 = new MySqlCommand("select * from dosyalar where completed=0", conn);
            MySqlDataReader datareader = varmi2.ExecuteReader();
            MySqlConnection conni = new MySqlConnection("server=localhost;Port=3306;User=root;database=santral;password=1234");
            conni.Open();
            while (datareader.Read())
            {
              
                string path = datareader["RELATIVE_PATH"].ToString();
                DirectoryInfo di = new DirectoryInfo(path);
                FileInfo[] rgfiles2 = di.GetFiles(datareader["NAME"].ToString());


                OleDbConnection connole = new OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0; Data Source=" + datareader["SERVER_CLIENT"].ToString());

              
                foreach (FileInfo d2 in rgfiles2)
                {

                    connole.Open();

                    OleDbCommand cmd = new OleDbCommand("SELECT * FROM recindex", connole);
                    cmd.CommandType = System.Data.CommandType.TableDirect;

                    OleDbDataReader dr = cmd.ExecuteReader();

                    List<string> liste = new List<string>();

                    while (dr.Read())
                    {
                     string key = dr["date1"].ToString() + dr["TIME1"].ToString() + dr["TIME2"].ToString() + dr["date1"].ToString();

                        System.Text.UnicodeEncoding obje = new System.Text.UnicodeEncoding();
                        byte[] bytString = obje.GetBytes(key);
                        MD5CryptoServiceProvider objProv = new MD5CryptoServiceProvider();
                        byte[] hash = objProv.ComputeHash(bytString);
                        string hashkey = Convert.ToBase64String(hash);
                        

                        liste.Add(dr["line1"].ToString());
                        liste.Add(dr["date1"].ToString());
                        liste.Add(dr["date1"].ToString());
                        liste.Add(dr["TIME1"].ToString());
                        liste.Add(dr["TIME2"].ToString());
                        liste.Add(dr["line1"].ToString());
                        liste.Add(dr["CALLER"].ToString());
                        liste.Add(dr["ext"].ToString());
                        liste.Add(dr["DURATION"].ToString());
                        liste.Add(dr["fname"].ToString());
                        liste.Add(dr["AGENT"].ToString());
                        liste.Add(dr["GROUP1"].ToString());
                        liste.Add(hashkey);


                        MySqlConnection conn2 = new MySqlConnection("server=localhost;Port=3306;User=root;database=santral;password=1234");
                        conn2.Open();

                        string keysorgu = "select count(_key) as sayi from kayit where _key='" + hashkey + "'";

                        MySqlCommand keycmd = new MySqlCommand(keysorgu, conn2);
                        int sayi = Convert.ToInt32(keycmd.ExecuteScalar());
                      

                        if (sayi < 1)
                        {
                           
                            String srg = "INSERT INTO kayit (KANAL_ID, TIP, BASLANGIC_TARIH, BITIS_TARIH, BASLANGIC_SAAT, BITIS_SAAT, DAHILI, ARAYAN, ARANAN, SURE, KAYIT_DOSYA, AGENT, _GROUP,_KEY) VALUES ('" + dr["line1"] + "','" + "NULL" + "','" + dr["date1"].ToString() + "' , '" + dr["date1"].ToString() + "' , '" + dr["TIME1"].ToString() + "' , '" + dr["TIME2"].ToString() + "' , '" + dr["line1"].ToString() + "' , '" + dr["CALLER"].ToString() + "' , '" + dr["ext"].ToString() + "' , '" + dr["DURATION"].ToString() + "' , '" + dr["fname"].ToString() + "' , '" + dr["AGENT"].ToString() + "' , '" + dr["GROUP1"].ToString() + "' , '" + hashkey + "')";

                            MySqlCommand cmd2 = new MySqlCommand(srg, conn2);
                            cmd2.ExecuteNonQuery();        
                        }
                        
                        conn2.Close();         
                    }
                    dr.Close();
                    cmd.ExecuteNonQuery();
                }
               
                MySqlCommand exectrue = new MySqlCommand("update dosyalar set completed = 1 where NAME='" + datareader["NAME"].ToString() + "'", conni);
                exectrue.ExecuteNonQuery();
                
            }
            conni.Close();
            datareader.Close();
            conn.Close();
            return 1;
           
        }
     
        public static void channelYazdir()
        {
            MySqlConnection conn = new MySqlConnection("server=localhost;Port=3306;User=root;database=santral;password=1234");
            conn.Open();

            OleDbConnection connole = new OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0; Data Source=C://genex/recIndeX.mdb");
            connole.Open();

            OleDbCommand cmd = new OleDbCommand();
            cmd.Connection = connole;
            cmd.CommandText = "channels";
            cmd.CommandType = System.Data.CommandType.TableDirect;
            OleDbDataReader dr = cmd.ExecuteReader();

            List<string> liste = new List<string>();


            while (dr.Read())
            {

                liste.Add(dr["line1"].ToString());
                liste.Add(dr["exten"].ToString());
                liste.Add(dr["name"].ToString());
                liste.Add(dr["time2"].ToString());
                liste.Add(dr["record1"].ToString());
                liste.Add(dr["time1"].ToString());
                liste.Add(dr["group"].ToString());
                liste.Add(dr["interval1"].ToString());
                liste.Add(dr["durec"].ToString());
                liste.Add(dr["tcpip"].ToString());
                liste.Add(dr["actlow"].ToString());
                liste.Add(dr["acthigh"].ToString());

                string key = dr["line1"].ToString() + dr["exten"].ToString() + dr["name"].ToString() + dr["time2"].ToString() + dr["record1"].ToString() + dr["time1"].ToString() + dr["group"].ToString() + dr["interval1"].ToString() + dr["durec"].ToString() + dr["tcpip"].ToString() + dr["actlow"].ToString() + dr["acthigh"].ToString();

                System.Text.UnicodeEncoding obje = new System.Text.UnicodeEncoding();
                byte[] bytString = obje.GetBytes(key);
                MD5CryptoServiceProvider objProv = new MD5CryptoServiceProvider();
                byte[] hash = objProv.ComputeHash(bytString);
                string hashkey = Convert.ToBase64String(hash);

                string keysorgu = "select count(_key) as sayi from kanal where _key='" + hashkey + "'";
                MySqlCommand keycmd = new MySqlCommand(keysorgu, conn);
                int sayi = Convert.ToInt32(keycmd.ExecuteScalar());

                if (sayi < 1)
                {

                    MySqlCommand cmd2 = new MySqlCommand("insert into kanal(KANAL_ID,CH_NUMBER,CH_NAME,CAPTION,_KEY) values(@KANAL_ID,@CH_NUMBER,@CH_NAME,@CAPTION,@_KEY)", conn);

                    cmd2.Parameters.AddWithValue("@KANAL_ID", dr["line1"].ToString());
                    cmd2.Parameters.AddWithValue("@CH_NUMBER", dr["exten"].ToString());
                    cmd2.Parameters.AddWithValue("@CH_NAME", dr["name"].ToString());
                    cmd2.Parameters.AddWithValue("@CAPTION", dr["name"].ToString());
                    cmd2.Parameters.AddWithValue("@_KEY", hashkey);
                    cmd2.ExecuteNonQuery();

                }
            }
            connole.Close();
            conn.Close();

          
        }
    }
}





