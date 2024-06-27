import de.hpi.dbs1.ChosenImplementation;
import de.hpi.dbs1.ConnectionConfig;
import de.hpi.dbs1.JDBCExercise;
import de.hpi.dbs1.entities.Actor;
import de.hpi.dbs1.entities.Movie;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

@ChosenImplementation(true)
public class JDBCExerciseJavaImplementation implements JDBCExercise {

	Logger logger = Logger.getLogger(this.getClass().getSimpleName());

	@Override
	public Connection createConnection(@NotNull ConnectionConfig config) throws SQLException {
		String URL = "jdbc:postgresql://localhost:5432/IMDB";
		String name = "user";
		String pw = "test";
		return DriverManager.getConnection(URL, name, pw);
	}

	@Override
	public List<Movie> queryMovies(
			@NotNull Connection connection,
			@NotNull String keywords
	) throws SQLException {
		logger.info(keywords);
		List<Movie> movies = new ArrayList<>();

		PreparedStatement pstmt = connection.prepareStatement(
				"SELECT DISTINCT tmovies.tconst, tmovies.\"primaryTitle\", tmovies.\"startYear\", tmovies.genres, nbasics.primaryname " +
						"FROM tmovies NATURAL JOIN tprincipals NATURAL JOIN nbasics " +
						"WHERE tmovies.\"primaryTitle\" LIKE ? AND (tprincipals.category = 'actor' OR tprincipals.category = 'actress')" +
						"ORDER BY tmovies.\"primaryTitle\" ASC, tmovies.\"startYear\" ASC, nbasics.primaryname ASC;"
		);
		pstmt.setString(1, "%" + keywords + "%");

		ResultSet rs = pstmt.executeQuery();

		String previousMovie = rs.getString(1);;
		String currentMovie = " ";
		var movie = new Movie(rs.getString(1), rs.getString(2), rs.getInt(3), Collections.singleton(rs.getString(4)));

		while (rs.next()) {
			currentMovie = rs.getString(1);
			if(previousMovie.equals(currentMovie)) {
				movie.actorNames.add(rs.getString(5));
			}else{
				movie = new Movie(rs.getString(1), rs.getString(2), rs.getInt(3), Collections.singleton(rs.getString(4)));
				previousMovie = currentMovie;
			}
			movies.add(movie);
		}

		/*
			ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		System.out.println(columnCount);
				for (int i = 1; i <= columnCount; i++) {
				System.out.print(rs.getString(i) + " ");
			}
			System.out.println();


		var myMovie = new Movie("??????????", "My Movie", 2023, Set.of("Indie"));
		myMovie.actorNames.add("Myself");
		movies.add(myMovie);
		*/

		return movies;
	}

	@Override
	public List<Actor> queryActors(
		@NotNull Connection connection,
		@NotNull String keywords
	) throws SQLException {
		logger.info(keywords);
		List<Actor> actors = new ArrayList<>();
		return actors;
		//throw new UnsupportedOperationException("Not yet implemented");
	}

}
