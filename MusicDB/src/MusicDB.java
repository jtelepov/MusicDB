import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MusicDB {

    public static final String DATABASE_NAME = "music.db";


    public static final String CONNECTION_PATH = "jdbc:sqlite:C:\\Users\\JTELEPOV\\Desktop\\Java\\SQLiteStudio\\" + DATABASE_NAME;

    public static final String ALBUMS_TABLE = "albums";
    public static final String ALBUMS_ID = "_id";
    public static final String ALBUMS_NAME = "name";
    public static final String ALBUMS_ARTIST = "artist";

    public static final String SONGS_TABLE = "songs";
    public static final String SONGS_ID = "_id";
    public static final String SONGS_TRACK = "track";
    public static final String SONGS_TITLE = "title";
    public static final String SONGS_ALBUM_ID = "album";

    public static final String ARTIST_TABLE = "artists";
    public static final String ARTISTS_ID = "_id";
    public static final String ARTISTS_NAME = "name";

    public static final int ORDER_BY_NONE = 1;
    public static final int ORDER_BY_ASC = 2;
    public static final int ORDER_BY_DESC = 3;

    public static final String querySong = "SELECT "+ SONGS_TITLE +" FROM "+ SONGS_TABLE +" WHERE "+SONGS_TITLE + " =?";

    public static final String QUERY_ALBUMS_BY_ARTIST_START =
            "SELECT " + ALBUMS_TABLE + '.' + ALBUMS_NAME + " FROM " + ALBUMS_TABLE +
                    " INNER JOIN " + ARTIST_TABLE + " ON " + ALBUMS_TABLE + "." + ALBUMS_ID +
                    " = " + ARTIST_TABLE + "." + ARTISTS_ID +
                    " WHERE " + ARTIST_TABLE + "." + ARTISTS_NAME + " = \"";

    public static final String QUERY_ALBUMS_BY_ARTIST_SORT =
            " ORDER BY " + ALBUMS_TABLE + "." + ALBUMS_NAME + " COLLATE NOCASE ";

    public static final String QUERY_ARTIST_FOR_SONG_START =
            "SELECT " + ARTIST_TABLE + "." + ARTISTS_NAME + ", " +
                    ALBUMS_TABLE + "." + ALBUMS_NAME + ", " +
                    SONGS_TABLE + "." + SONGS_TRACK + " FROM " + SONGS_TABLE +
                    " INNER JOIN " + ALBUMS_TABLE + " ON " +
                    SONGS_TABLE + "." + SONGS_ALBUM_ID + " = " + ALBUMS_TABLE + "." + ALBUMS_ID +
                    " INNER JOIN " + ARTIST_TABLE + " ON " +
                    ALBUMS_TABLE + "." + ALBUMS_ID + " = " + ARTIST_TABLE + "." + ARTISTS_ID +
                    " WHERE " + SONGS_TABLE + "." + SONGS_TITLE + " = \"";

    public static final String QUERY_ARTIST_FOR_SONG_SORT =
            " ORDER BY " + ARTIST_TABLE + "." + ARTISTS_NAME + ", " +
                    ALBUMS_TABLE + "." + ALBUMS_NAME + " COLLATE NOCASE ";

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";

    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE VIEW IF NOT EXISTS " +
            TABLE_ARTIST_SONG_VIEW + " AS SELECT " + ARTIST_TABLE + "." + ARTISTS_NAME + ", " +
            ALBUMS_TABLE + "." + ALBUMS_NAME + " AS " + SONGS_ALBUM_ID + ", " +
            SONGS_TABLE + "." + SONGS_TRACK + ", " + SONGS_TABLE + "." + SONGS_TITLE +
            " FROM " + SONGS_TABLE +
            " INNER JOIN " + ALBUMS_TABLE + " ON " + SONGS_TABLE +
            "." + SONGS_ALBUM_ID + " = " + ALBUMS_TABLE + "." + ALBUMS_ID +
            " INNER JOIN " + ARTIST_TABLE + " ON " + ALBUMS_TABLE + "." + ALBUMS_NAME +
            " = " + ARTIST_TABLE + "." + ARTISTS_ID +
            " ORDER BY " +
            ARTIST_TABLE + "." + ARTISTS_NAME + ", " +
            ALBUMS_TABLE + "." + ALBUMS_NAME + ", " +
            SONGS_TABLE + "." + SONGS_TRACK;

    public static final String QUERY_VIEW_SONG_INFO = "SELECT " + ARTISTS_NAME + ", " +
            SONGS_ALBUM_ID + ", " + SONGS_TRACK + " FROM " + TABLE_ARTIST_SONG_VIEW +
            " WHERE " + SONGS_TITLE + " = \"";

    public static final String QUERY_VIEW_SONG_INFO_PREP = "SELECT " + ARTISTS_NAME + ", " +
            SONGS_ALBUM_ID + ", " + SONGS_TRACK + " FROM " + TABLE_ARTIST_SONG_VIEW +
            " WHERE " + SONGS_TITLE + " = ?";


    public static final String INSERT_ARTIST = "INSERT INTO " + ARTIST_TABLE +
            '(' + ARTISTS_NAME + ") VALUES(?)";
    public static final String INSERT_ALBUMS = "INSERT INTO " + ALBUMS_TABLE +
            '(' + ALBUMS_NAME + ", " + ALBUMS_NAME + ") VALUES(?, ?)";

    public static final String INSERT_SONGS = "INSERT INTO " + SONGS_TABLE +
            '(' + SONGS_TRACK + ", " + SONGS_TITLE + ", " + SONGS_ALBUM_ID +
            ") VALUES(?, ?, ?)";

    public static final String QUERY_ARTIST = "SELECT " + ARTISTS_ID + " FROM " +
            ARTIST_TABLE + " WHERE " + ARTISTS_NAME + " = ?";

    public static final String QUERY_ALBUM = "SELECT " + ALBUMS_ID + " FROM " +
            ALBUMS_TABLE + " WHERE " + ALBUMS_NAME + " = ?";

    private Connection conn;

    private PreparedStatement querySongInfoView;

    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;

    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;


    public boolean open() {
        try {
            conn = DriverManager.getConnection(CONNECTION_PATH);
            insertIntoArtists = conn.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums = conn.prepareStatement(INSERT_ALBUMS, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = conn.prepareStatement(INSERT_SONGS);
            queryArtist = conn.prepareStatement(QUERY_ARTIST);
            queryAlbum = conn.prepareStatement(QUERY_ALBUM);


            return true;
        } catch (SQLException e) {
            System.out.println("Couldn't connect to database: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public void close() {
        try {

            if(querySongInfoView != null) {
                querySongInfoView.close();
            }

            if(insertIntoArtists != null) {
                insertIntoArtists.close();
            }

            if(insertIntoAlbums != null) {
                insertIntoAlbums.close();
            }

            if(insertIntoSongs !=  null) {
                insertIntoSongs.close();
            }

            if(queryArtist != null) {
                queryArtist.close();
            }

            if(queryAlbum != null) {
                queryAlbum.close();
            }

            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            System.out.println("Couldn't close connection: " + e.getMessage());
        }
    }

    public List<Artist> queryArtists(int sortOrder) {

        StringBuilder sb = new StringBuilder("SELECT * FROM ");
        sb.append(ARTIST_TABLE);
        if (sortOrder != ORDER_BY_NONE) {
            sb.append(" ORDER BY ");
            sb.append(ARTISTS_NAME);
            sb.append(" COLLATE NOCASE ");
            if (sortOrder == ORDER_BY_DESC) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<Artist> artists = new ArrayList<>();
            while (results.next()) {
                Artist artist = new Artist();
                artist.setId(results.getInt(ARTISTS_ID));
                artist.setName(results.getString(ARTISTS_NAME));
                artists.add(artist);
            }

            return artists;

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }

    public List<String> queryAlbumsForArtist(String artistName, int sortOrder) {
        StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START);
        sb.append(artistName);
        sb.append("\"");

        if (sortOrder != ORDER_BY_NONE) {
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if (sortOrder == ORDER_BY_DESC) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }

        System.out.println("SQL statement = " + sb.toString());
        System.out.println("Album for artist "+ artistName+":");

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<String> albums = new ArrayList<>();
            while (results.next()) {
                albums.add(results.getString(1));
            }

            return albums;

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }

    public List<SongArtist> queryArtistsForSong(String songName, int sortOrder) {

        StringBuilder sb = new StringBuilder(QUERY_ARTIST_FOR_SONG_START);
        sb.append(songName);
        sb.append("\"");

        if (sortOrder != ORDER_BY_NONE) {
            sb.append(QUERY_ARTIST_FOR_SONG_SORT);
            if (sortOrder == ORDER_BY_DESC) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }

        System.out.println("SQL Statement: " + sb.toString());

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<SongArtist> songArtists = new ArrayList<>();

            while (results.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(results.getString(1));
                songArtist.setAlbumName(results.getString(2));
                songArtist.setTrack(results.getInt(3));
                songArtists.add(songArtist);
            }

            return songArtists;
        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }


    public int getCount(String table) {
        String sql = "SELECT COUNT(*) AS count FROM " + table;
        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sql)) {

            int count = results.getInt("count");

            System.out.format("Count = %d\n", count);
            return count;
        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return -1;
        }
    }

    public boolean createViewForSongArtists() {

        try (Statement statement = conn.createStatement()) {

            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            return true;

        } catch (SQLException e) {
            System.out.println("Create View failed: " + e.getMessage());
            return false;
        }
    }

    public List<SongArtist> querySongInfoView(String title) {

        try {
            querySongInfoView.setString(1, title);
            ResultSet results = querySongInfoView.executeQuery();

            List<SongArtist> songArtists = new ArrayList<>();
            while (results.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(results.getString(1));
                songArtist.setAlbumName(results.getString(2));
                songArtist.setTrack(results.getInt(3));
                songArtists.add(songArtist);
            }

            return songArtists;

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }

    private int insertArtist(String name) throws SQLException {

        queryArtist.setString(1, name);
        ResultSet results = queryArtist.executeQuery();
        if(results.next()) {
            return results.getInt(1);
        } else {
            // Insert the artist
            insertIntoArtists.setString(1, name);
            int affectedRows = insertIntoArtists.executeUpdate();

            if(affectedRows != 1) {
                throw new SQLException("Couldn't insert artist!");
            }

            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();
            if(generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for artist");
            }
        }
    }

    private int insertAlbum(String name, int artistId) throws SQLException {

        queryAlbum.setString(1, name);
        ResultSet results = queryAlbum.executeQuery();
        if(results.next()) {
            return results.getInt(1);
        } else {
            // Insert the album
            insertIntoAlbums.setString(1, name);
            insertIntoAlbums.setInt(2, artistId);
            int affectedRows = insertIntoAlbums.executeUpdate();

            if(affectedRows != 1) {
                throw new SQLException("Couldn't insert album!");
            }

            ResultSet generatedKeys = insertIntoAlbums.getGeneratedKeys();
            if(generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for album");
            }
        }
    }

    public void insertSong(String title, String artist, String album, int track) {

        try {
            conn.setAutoCommit(false);

            int artistId = insertArtist(artist);
            int albumId = insertAlbum(album, artistId);
            insertIntoSongs.setInt(1, track);
            insertIntoSongs.setString(2, title);
            insertIntoSongs.setInt(3, albumId);
            int affectedRows = insertIntoSongs.executeUpdate();
            if(affectedRows == 1) {
                conn.commit();
            } else {
                throw new SQLException("The song insert failed");
            }

        } catch(Exception e) {
            System.out.println("Insert song exception: " + e.getMessage());
            try {
                System.out.println("Performing rollback");
                conn.rollback();
            } catch(SQLException e2) {
                System.out.println("Oh boy! Things are really bad! " + e2.getMessage());
            }
        } finally {
            try {
                System.out.println("Resetting default commit behavior");
                conn.setAutoCommit(true);
            } catch(SQLException e) {
                System.out.println("Couldn't reset auto-commit! " + e.getMessage());
            }

        }
    }



}
