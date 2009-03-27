package org.drizzle.jdbc.internal.queryresults;

import org.drizzle.jdbc.internal.ValueObject;

/**
 * User: marcuse
 * Date: Mar 9, 2009
 * Time: 8:42:45 PM

 */
public interface SelectQueryResult extends QueryResult {


    /**
     * gets the value object at position index, starts at 0
     * @param index the position, starts at 0
     * @return the value object at position index 
     *  @throws NoSuchColumnException if the column does not exist
     * */
    ValueObject getValueObject(int index) throws NoSuchColumnException;

    /**
     * gets the value object in column named columnName
     * @param columnName the name of the column
     * @return a value object
     *  @throws NoSuchColumnException if the column does not exist
     */
    ValueObject getValueObject(String columnName) throws NoSuchColumnException;

    /**
     * get the id of the column named columnLabel
     * @param columnLabel the label of the column
     * @return the index, starts at 0
     *  @throws NoSuchColumnException if the column does not exist
     */
    int getColumnId(String columnLabel) throws NoSuchColumnException;

    /**
     * moves the row pointer to position i
     * @param i the position
     */
    void moveRowPointerTo(int i);

    /**
     * gets the current row number
     * @return the current row number
     */
    int getRowPointer();

    /**
     * move pointer forward
     * @return true if there is another row
     */
    boolean next();
}
