package superheroes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SuperheroesDatabase {
	//Constantes:
	//Constantes para el establecimiento de la conexion
	private static final String SERVER_ADDRESS = "localhost:3306";
	private static final String DB = "superheroes";
	private static final String USER = "superheroes_user";  
	private static final String PASS = "superheroes_pass";  
	private static final String URL = "jdbc:mysql://" + SERVER_ADDRESS + "/" + DB;
	private static final String DRV = "com.mysql.cj.jdbc.Driver";
	//Constantes que indican el mensaje de error que se lanza
	private static final String ERR_MESSAGE_SQL = "SQL Error Message: %s; Code: %s; SQL state: %s\n";
	private static final String FILE_NOT_FOUND = "File %s was not found";
	private static final String ERR_IO_MESSAGE = "IO Error Message: %s";
	//Variables:
	private Connection conn = null;
	public SuperheroesDatabase() {
	}
	
	//Primera sesión:
	//Metodos publicos:
	/**
	 * openConnection abre la conexion con la base de datos
	 * @return true si la apertura se ha hecho correctamente y si no habia ya una conexion abierta
	 */
	public boolean openConnection() {
		if(conn != null) {
			return false;
		}
		try {
			Class.forName(DRV);
			conn = DriverManager.getConnection(URL, USER, PASS);
			return true;
		} catch (SQLException e) {
			System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
		} catch (ClassNotFoundException e) {
			System.err.println("Driver not found");
		} 
		return false;
	}
	
	/**
	 * closeConnection() cierra la conexion con la base de datos
	 * @return true si se ha cerrado correctamente y la conexion no estaba ya cerrada
	 */
	public boolean closeConnection() {
		if(conn == null) {
			return true;
		}
		try {
			conn.close();
			conn = null;
			return true;
		} catch (SQLException e) {
			System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
		}
		return false;
	}
	
	/**
	 * createTableEscena() crea la tabla escena con sus atributos id_pelicula(PK, FK), n_orden(PK), titulo y duracion
	 * @return true si se ha creado de forma correcta la tabla y la tabla no existia en la base de datos
	 */
	public boolean createTableEscena() {
		String tableName = "escena";
		String createTbEscena = "CREATE TABLE escena (id_pelicula INT, n_orden INT, titulo VARCHAR(100), duracion INT, PRIMARY KEY(id_pelicula, n_orden), "
				+ "FOREIGN KEY(id_pelicula) references pelicula(id_pelicula) ON UPDATE CASCADE ON DELETE CASCADE);";
		return createTable(tableName, createTbEscena);
	}
	
	/**
	 * createTableRival() crea la tabla del rival con sus atributos id_sup(PK, FK), id_villano(PK, FK) y fecha_primer_encuentro
	 * @return true si se realiza la creacion
	 */
	public boolean createTableRival() {
		String tableName = "rival";
		String createTbRival = "CREATE TABLE rival (id_sup INT, id_villano INT, fecha_primer_encuentro DATE, PRIMARY KEY(id_sup, id_villano), "
				+ "FOREIGN KEY(id_sup) references superheroe(id_sup) ON DELETE CASCADE ON UPDATE CASCADE,"
				+ "FOREIGN KEY(id_villano) references villano(id_villano) ON DELETE CASCADE ON UPDATE CASCADE);";
		return createTable(tableName, createTbRival);
	}
	
	//Metodos privados:
	/**
	 * createTable() crea una tabla de nombre tableName usando la query
	 * @param tableName
	 * @param query
	 * @return true si se ha creado correctamente
	 */
	private boolean createTable(String tableName, String query) {
		if(!openConnection() && conn == null || tableExists(tableName)) {
			return false;
		}
		Statement st = null;
		try {
			st = conn.createStatement();
			st.executeUpdate(query);
			st.close();
			return true;
		} catch (SQLException se) {
			System.err.printf(ERR_MESSAGE_SQL, se.getMessage(), se.getErrorCode(), se.getSQLState());
		} finally {
			try {
				if(st != null) st.close();
			} catch(SQLException se) {
				System.err.printf(ERR_MESSAGE_SQL, se.getMessage(), se.getErrorCode(), se.getSQLState());
			}
		}
		return false;
	}
	
	/**
	 * tableExists() comprueba que la tabla de nombre tableName existe en la base de datos
	 * @param tableName
	 * @return true si existe y se comprueba sin ningun error
	 */
	private boolean tableExists(String tableName) {
		ResultSet rs = null;
		try {
			DatabaseMetaData dbMet = conn.getMetaData();
			rs = dbMet.getTables(null, DB, tableName, null);
			boolean exists = rs.next();
			rs.close();
			return exists;
		} catch (SQLException se) {
			System.err.printf(ERR_MESSAGE_SQL, se.getMessage(), se.getErrorCode(), se.getSQLState());
		} finally {
			try {
				if(rs != null) rs.close();
			} catch (SQLException se) {
				System.err.printf(ERR_MESSAGE_SQL, se.getMessage(), se.getErrorCode(), se.getSQLState());
			}
		}
		return false;
	}
	
	//Segunda sesion:
	//Metodos publicos:
	/**
	 * loadEscenas carga los valores dados en el archivo al que apunta fileName 
	 * en la tabla escenas, usa el autoCommit, es decir, por cada linea se realiza un commit
	 * y si una linea o mas lineas no se pueden introducir salta a la siguiente y continua
	 * @param fileName
	 * @return Devuelve el numero de lineas introducidas en la tabla
	 */
	public int loadEscenas(String fileName) {
		if(!openConnection() && conn == null) {
			return 0;
		}
		String insertIntoEscenas = "INSERT INTO escena(id_pelicula, n_orden, titulo, duracion) values(?, ?, ?, ?);";
		PreparedStatement pst = null;
		try(BufferedReader br = new BufferedReader(new FileReader(fileName));) {
			pst =  conn.prepareStatement(insertIntoEscenas);
			String linea = br.readLine();
			int total = 0;
			while(linea != null) {
				if(insertEscena(linea, pst)) {
					total++;
				}
				linea = br.readLine();
			}
			pst.close();
			return total;
		} catch (FileNotFoundException e) {
			System.err.printf(FILE_NOT_FOUND, fileName);
		} catch (IOException e) {
			System.err.printf(ERR_IO_MESSAGE, e.getMessage());
		} catch (SQLException e) {
			System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
		} finally {
			try {
				if(pst != null) pst.close();
			} catch (SQLException e) {
				System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
			}
		}
		return 0;
	}

	/**
	 * loadProtagoniza carga los datos del archivo de nombre fileName o el path al archivo 
	 * en la tabla protagoniza y los pares unicos de superheroe y villano los anade a la 
	 * tabla rival (no se introduce la fecha del primer encuentro, se deja como null)
	 * Si algunas de las inserciones falla, no se inserta ninguna de las anteriores
	 * @param fileName
	 * @return suma del numero de inserciones de la tabla protagoniza y de la de rival
	 */
	public int loadProtagoniza(String fileName) {
		if(!openConnection() && conn == null) {
			return 0;
		}
		String insertIntoProtagoniza = "INSERT INTO protagoniza(id_sup, id_villano, id_pelicula) values(?, ?, ?);";
		String insertIntoRival = "INSERT INTO rival(id_sup, id_villano) values(?, ?);";
		List<String> supVill = new ArrayList<>();
		PreparedStatement pst = null;
		try {
			conn.setAutoCommit(false);
			pst =  conn.prepareStatement(insertIntoProtagoniza);
			int total = insertProtagoniza(fileName, pst, supVill);
			pst.close();
			boolean allRight = total != -1;
			if(allRight) {
				pst = conn.prepareStatement(insertIntoRival);
				total += supVill.size();
				for(int i = 0; i < supVill.size() && allRight; i++) {
					allRight = insert(pst, supVill.get(i).split(","));
				}
				pst.close();  
			}
			if(!allRight) {
				conn.rollback();
			} else {
				conn.commit();
			}
			return allRight? total: 0;
		} catch (SQLException e) {
			System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
		} finally {
			try {
				conn.setAutoCommit(true);
				if(pst != null) pst.close();
			} catch (SQLException e) {
				System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
			}
		}
		return 0;
	}
	
	//Metodos privados:
	/**
	 * insertEscena inserta los valores de la escena, almacenados en linea, en la 
	 * base de datos
	 * PRE: pst esta abierto previo a la ejecucion de esta instruccion
	 * @param linea
	 * @param pst
	 * @return true si se inserta correctamente, false en caso contrario
	 */
	private boolean insertEscena(String linea, PreparedStatement pst) {
		if(pst == null) return false;
		String[] values = linea.split(";");
		if(values.length != 4) {
			System.err.println("Not enough values");
			return false;
		}	
		try {
			pst.setInt(1, Integer.parseInt(values[0]));
			pst.setInt(2, Integer.parseInt(values[1]));
			pst.setString(3, values[2]);
			pst.setInt(4, Integer.parseInt(values[3]));
			pst.executeUpdate();
			return true;
		} catch(SQLException e) {
			System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
		}
		return false;
	}
	
	/**
	 * insertProtagoniza realiza las inserciones en la tabla protagoniza
	 * @param fileName es el nombre del arichivo o su path
	 * @param pst ha de estar abierto previo a la ejecucion de la funcion
	 * @param supVill lista en la que se almacenan los pares unicos de superheroe y villano
	 * @return numero de inserciones de la tabla protagoniza, -1 si no se ha podido realizar una insercion
	 */
	private int insertProtagoniza(String fileName, PreparedStatement pst, List<String> supVill) {
		if(pst == null) return -1;
		try(BufferedReader br = new BufferedReader(new FileReader(fileName));) {
			String linea = br.readLine();
			int total = 0;
			boolean allRight = true;
			while(linea != null && allRight) {
				String[] values = linea.split(";");
				allRight = insert(pst, values);
				linea = br.readLine();
				total++;
				String superVill = values[0] + "," + values[1];
				if(!supVill.contains(superVill)) {
					supVill.add(superVill);
				}
			}
			return allRight? total: -1;
		} catch (FileNotFoundException e) {
			System.err.printf(FILE_NOT_FOUND, fileName);
		} catch (IOException e) {
			System.err.printf(ERR_IO_MESSAGE, e.getMessage());
		}
		return -1;
	}
	
	/**
	 * insert realiza las inserciones numericas consecutivas a una tabla
	 * @param pst, ha de estar abierto previo a la funcion
	 * @param values valores numericos a insertar
	 * @return true si se ha realizado correctamente y no se han producido excepciones
	 */
	private boolean insert(PreparedStatement pst, String[] values) {
		if(pst == null) return false;
		try {
			for(int i = 0; i < values.length; i++) {
				pst.setInt(i + 1, Integer.parseInt(values[i]));
			}
			pst.executeUpdate();
			return true;
		} catch (SQLException e) {
			System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
		}
		return false;
		
	}
	
	//Tercera Sesion:
	//Metodos publicos:
	/**
	 * catalogo devuelve una cadena con el nombre de las peliculas registradas en la base de datos en el
	 * formato {nombre_pelicula_1, nombre_pelicula_2, ..., nombre_pelicula_n}, ordenadas alfabéticamente
	 * @return la cadena de las peliculas, {} si no hay peliculas o null si se ha producido una excepcion
	 */
	public String catalogo() {
		if(!openConnection() && conn == null) {
			return null;
		}
		String getTitulosPelicula = "SELECT titulo FROM pelicula ORDER BY titulo;";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = conn.createStatement();
			rs = st.executeQuery(getTitulosPelicula);
			String out = rsToString(rs);
			rs.close();	
			st.close();
			return out;
		} catch (SQLException e) {
			System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState()); 
		} finally {
			try {
				if(st != null) st.close();
				if(rs != null) rs.close();
			} catch (SQLException e) {
				System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
			}
		}
		return null; 
	}
	
	/**
	 * duracionPelicula devuelve la duracion en minutos de la pelicula
	 * @param nombrePelicula
	 * @return si la pelicula no está en la base de datos devuelve -1, si se produce una excepcion -2
	 */
	public int duracionPelicula(String nombrePelicula) {
		if(!openConnection() && conn == null) {
			return -2;
		}
		String getId = "SELECT id_pelicula from pelicula where titulo = ?";
		String getDuration = "SELECT sum(duracion) from escena where id_pelicula = ?;";
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			pst = conn.prepareStatement(getId);
			pst.setString(1, nombrePelicula);
			rs = pst.executeQuery();
			if(!rs.next()) {
				return -1;
			}
			int idPelicula = rs.getInt(1);
			rs.close();
			pst.close();
			pst = conn.prepareStatement(getDuration);
			pst.setInt(1, idPelicula);
			rs = pst.executeQuery();
			int duracion = rs.next()? rs.getInt(1): 0;
			rs.close();
			pst.close();
			return duracion;
		} catch (SQLException e) {
			System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
		} finally {
			try {
				if(pst != null) pst.close();
				if(rs != null) rs.close();
			} catch (SQLException e) {
				System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
			}
		}
		return -2;
		
	}
	
	/**
	 * getEscenas devuelve una cadena en el formato {nombre_escena_1, nombre_escena_2, ..., nombre_escena_n}
	 * de todas las escenas; ordenadas alfabéticamente, en las que aparezca el villano nombrado
	 * @param nombreVillano
	 * @return {nombre_escena_1, nombre_escena_2, ..., nombre_escena_n}, {} si el villano no existe en la base de datos o no tiene escenas
	 * 			y null si se ha producido alguna excepcion
	 */
	public String getEscenas(String nombreVillano) {
		if(!openConnection() && conn == null) {
			return null;
		}
		String getTitulosFromEscena = "SELECT titulo from escena e, villano v, protagoniza pr where v.id_villano = pr.id_villano AND e.id_pelicula =  pr.id_pelicula AND v.nombre = ?" +
			" ORDER BY titulo;";
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			pst = conn.prepareStatement(getTitulosFromEscena);
			pst.setString(1, nombreVillano);
			rs = pst.executeQuery();
			String escenas = rsToString(rs);
			rs.close();
			pst.close();
			return escenas;
		} catch (SQLException e) {
			System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
		} finally {
			try {
				if(pst != null) pst.close();
				if(rs != null) rs.close();
			} catch (SQLException e) {
				System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
			}
		}
		return null;
	}
	
	//Metodo privado:
	/**
	 * rsToString lee los resultados de un resultSet y los escribe en el formato {rs_1, rs_2, ..., rs_n}
	 * Si esta vacio devuelve {} y si en el rs se produce alguna excepcion null
	 * @param rs ResultSet ya iniciado en alguna operación anterior
	 * @return {rs_1, rs_2, ..., rs_n}
	 */
	private String rsToString(ResultSet rs) {
		if(rs == null)	return null;	
		List<String> list = new ArrayList<>();
		try {
			while(rs.next()) {
				list.add(rs.getString(1));
			}
		} catch (SQLException e) {
			System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
			return null;
		}
		
		if(list.isEmpty()) {
			return "{}";
		}
		String out = "{" + list.get(0);
		int size = list.size();
		for(int i = 1; i < size; i++) {
			out += ", " + list.get(i);
		}
		return out + "}";
	}
	
	//Cuarta Sesion:
	//Metodos publicos:
	/**
	 * desenmascarar obtiene la imagen del avatar del superheroe que enmascara a la 
	 * persona real, cuyo nombre y apellidos se pasan
	 * @param nombre
	 * @param apellido
	 * @param fileName
	 * @return true, si exite la imagen en la base de datos y se ha almacenado correctamente, false en caso contrario
	 */
	public boolean desenmascara(String nombre, String apellido, String fileName) {
		if(!openConnection() && conn == null) {
			return false;
		}
		String getAvatar = "SELECT avatar FROM superheroe sp, persona_real pr where sp.id_persona = pr.id_persona AND pr.nombre = ? AND pr.apellido = ?;";
		byte[] data = getData(nombre, apellido, getAvatar);
		if(data == null) {
			return false;
		}
		try(FileOutputStream fos = new FileOutputStream(fileName);) {
			fos.write(data);
			return true;
		} catch (FileNotFoundException e) {
			System.err.printf(FILE_NOT_FOUND, fileName);
		} catch (IOException e) {
			System.err.printf(ERR_IO_MESSAGE, e.getMessage());
		}
		return false;
	}
	
	//Metodos privados:
	/**
	 * getData obtiene los bytes que contienen la informacion de la imagen
	 * @param nombre
	 * @param apellido
	 * @param getFile es la SQL request
	 * @return el data[] de sus bytes si la imagen esta en la base de datos, null en caso contrario o si se ha producido una excepcion
	 */
	private byte[] getData(String nombre, String apellido, String getFile) {
		PreparedStatement pst = null;
		ResultSet rs = null;
		Blob myBlob = null;
		try {
			pst = conn.prepareStatement(getFile);
			pst.setString(1, nombre);
			pst.setString(2, apellido);
			rs = pst.executeQuery();
			if(rs.next()) {
				myBlob = rs.getBlob(1);
			}
			rs.close();
			pst.close();
			return myBlob != null? myBlob.getBytes(1, (int)myBlob.length()): null;
		} catch (SQLException e) {
			System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
		} finally {
			try {
				if(pst != null) pst.close();
				if(rs != null) rs.close();
			} catch (SQLException e) {
				System.err.printf(ERR_MESSAGE_SQL, e.getMessage(), e.getErrorCode(), e.getSQLState());
			}
		}
		return null;
	}

}