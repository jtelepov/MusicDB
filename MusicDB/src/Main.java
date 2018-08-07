import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Main {


    public static void main(String[] args) {
        MusicDB MusicDB = new MusicDB();
        if(!MusicDB.open()) {
            System.out.println("Can't open MusicDB");
            return;
        }

        List<Artist> artists = MusicDB.queryArtists(1);
        if(artists == null) {
            System.out.println("No artists!");
            return;
        }
        System.out.println("All artists: ");
        for(Artist artist : artists) {
            System.out.println("ID = " + artist.getId() + ", Name = " + artist.getName());
        }
        System.out.println("*********************************************************");
        System.out.println("\n\n");

        List<String> albumsForArtist = MusicDB.queryAlbumsForArtist("Iron Maiden", MusicDB.ORDER_BY_ASC);
        for(String album : albumsForArtist) {
            System.out.println(album);
        }
        System.out.println("*********************************************************");
        System.out.println("\n\n");

        List<SongArtist> songArtists = MusicDB.queryArtistsForSong("Rat Salad", MusicDB.ORDER_BY_ASC);
        if(songArtists == null) {
            System.out.println("Couldn't find the artist for the song");
            return;
        }

        for(SongArtist artist : songArtists) {
            System.out.println("Artist name = " + artist.getArtistName() +
                    ", Album name = " + artist.getAlbumName() +
                    ", Track = " + artist.getTrack());
        }

        System.out.println("*********************************************************");
        System.out.println("\n\n");


        int count = MusicDB.getCount(MusicDB.SONGS_TABLE);
        System.out.println("Number of songs is: " + count);
        System.out.println("*********************************************************");
        System.out.println("\n\n");

        MusicDB.createViewForSongArtists();

//        Scanner scanner = new Scanner(System.in);
//        System.out.println("Enter a song title: ");
//        String title = scanner.nextLine();

//        songArtists = MusicDB.querySongInfoView(title);
//        if(songArtists.isEmpty()) {
//            System.out.println("Couldn't find the artist for the song");
//            return;
//        }
//
//        for(SongArtist artist : songArtists) {
//            System.out.println("FROM VIEW - Artist name = " + artist.getArtistName() +
//                " Album name = " + artist.getAlbumName() +
//                " Track number = " + artist.getTrack());
//        }

        MusicDB.insertSong("Bird Dog", "Everly Brothers", "All-Time Greatest Hits", 7);
        MusicDB.close();

    }
}
