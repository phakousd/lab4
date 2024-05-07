package edu.canisius.cyb600.lab4.database;

import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Category;
import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Posgres Implementation of the db adapter.
 */
public class PostgresDBAdapter extends AbstractDBAdapter {

    public PostgresDBAdapter(Connection conn) {
        super(conn);
    }

    public List<Actor> insertAllActorsWithAnOddNumberLastName(List<Actor> actors) {
        List<Actor> updatedActors = new ArrayList<>();
        String sql = "INSERT INTO ACTOR (FIRST_NAME, LAST_NAME) VALUES (?, ?) RETURNING ACTOR_ID, LAST_UPDATE;";

        for (Actor actor : actors) {
            if (actor.getLastName().length() % 2 == 1) {  // Check if the last name length is odd
                try (PreparedStatement statement = conn.prepareStatement(sql)) {
                    statement.setString(1, actor.getFirstName());
                    statement.setString(2, actor.getLastName());
                    ResultSet results = statement.executeQuery();
                    if (results.next()) {
                        actor.setActorId(results.getInt("ACTOR_ID"));
                        actor.setLastUpdate(results.getTimestamp("LAST_UPDATE"));
                        updatedActors.add(actor);  // Add to the list of updated actors
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return updatedActors;
    }
    public List<Film> getFilmsInCategory(Category category) {
        //Create a string with the sql statement
        String sql = "select *\n" +
                "from film " +
                "JOIN film_category ON film.film_id = film_category.film_id " +
                "JOIN category ON film_category.category_id = category.category_id " +
                "WHERE category.name = ?";

        //Prepare the SQL statement with the code
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            //Substitute a string for last name for the ? in the sql
            String categoryName = category.getName();
            statement.setString(1, categoryName);
            ResultSet results = statement.executeQuery();
            //Initialize an empty List to hold the return set of films.
            List<Film> films = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Film film = new Film();
                film.setFilmId(results.getInt("FILM_ID"));
                film.setTitle(results.getString("TITLE"));
                film.setDescription(results.getString("DESCRIPTION"));
                film.setReleaseYear(results.getString("RELEASE_YEAR"));
                film.setLanguageId(results.getInt("LANGUAGE_ID"));
                film.setRentalDuration(results.getInt("RENTAL_DURATION"));
                film.setRentalRate(results.getDouble("RENTAL_RATE"));
                film.setLength(results.getInt("LENGTH"));
                film.setReplacementCost(results.getDouble("REPLACEMENT_COST"));
                film.setRating(results.getString("RATING"));
                film.setSpecialFeatures(results.getString("SPECIAL_FEATURES"));
                film.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add film to the array
                films.add(film);
            }
            //Return all the films.
            return films;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
    public List<String> getAllDistinctCategoryNames(){
        //Create a string with the sql statement
        String sql = "select DISTINCT name FROM category";

        List<String> categoryNames = new ArrayList<>();

        //Prepare the SQL statement with the code
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
           ResultSet results = statement.executeQuery();
            //Loop through results and add each category name
            while (results.next()) {
                String categoryName = results.getString("name");
                categoryNames.add(categoryName);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return categoryNames;
    }
    public List<Film> getAllFilmsWithALengthLongerThanX(int length) {
        //Create a string with the sql statement
        String sql = "select *\n" +
                "from film " +
                "WHERE length > ?";

        //Initialize a list to hold films
        List<Film> films = new ArrayList<>();
        //Prepare the SQL statement with the code
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, length);
            ResultSet results = statement.executeQuery();
            // Loop through the results
            while (results.next()) {
                Film film = new Film();
                film.setFilmId(results.getInt("FILM_ID"));
                film.setTitle(results.getString("TITLE"));
                film.setDescription(results.getString("DESCRIPTION"));
                film.setReleaseYear(results.getString("RELEASE_YEAR"));
                film.setLanguageId(results.getInt("LANGUAGE_ID"));
                film.setRentalDuration(results.getInt("RENTAL_DURATION"));
                film.setRentalRate(results.getDouble("RENTAL_RATE"));
                film.setLength(results.getInt("LENGTH"));
                film.setReplacementCost(results.getDouble("REPLACEMENT_COST"));
                film.setRating(results.getString("RATING"));
                film.setSpecialFeatures(results.getString("SPECIAL_FEATURES"));
                film.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add film to the array
                films.add(film);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // Return films
        return films;
    }
    public List<Actor> getActorsFirstNameStartingWithX(char firstLetter){
        //Define the SQL query
        String sql = "select *\n" +
                "from actor " +
                "where first_name like ?";

        // Initialize a list to hold the actors
        List<Actor> actors = new ArrayList<>();

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, firstLetter + "%");

            // Execute the query and retrieve the result set
            ResultSet results = statement.executeQuery();

            while (results.next()) {
                Actor actor = new Actor();
                actor.setActorId(results.getInt("ACTOR_ID"));
                actor.setFirstName(results.getString("FIRST_NAME"));
                actor.setLastName(results.getString("LAST_NAME"));
                actor.setLastUpdate(results.getDate("LAST_UPDATE"));

                actors.add(actor);
            }
        } catch (SQLException e) {
            // Handle SQL exception
            throw new RuntimeException("Error fetching actors: ", e);
        }
        return actors;

    }
}




