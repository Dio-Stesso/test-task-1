package aa.intellias.testtask.operation.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import aa.intellias.testtask.collection.DataRow;
import aa.intellias.testtask.collection.JoinedDataRow;
import aa.intellias.testtask.operation.JoinOperation;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LeftJoinOperationTest {
    private JoinOperation<DataRow<Integer, String>, DataRow<Integer, String>,
            JoinedDataRow<Integer, String, String>> integerStringStringJoinOperation;
    private JoinOperation<DataRow<String, Integer>, DataRow<String, Integer>,
            JoinedDataRow<String, Integer, Integer>> stringIntegerIntegerJoinOperation;
    private DataRow<Integer, String> firstDataRow1;
    private DataRow<Integer, String> firstDataRow3;
    private DataRow<Integer, String> firstDataRow6;
    private DataRow<Integer, String> secondDataRow1;
    private DataRow<Integer, String> secondDataRow3;
    private JoinedDataRow<Integer, String, String> joinedDataRow1;
    private JoinedDataRow<Integer, String, String> joinedDataRow2;
    private JoinedDataRow<Integer, String, String> joinedDataRow3;

    @BeforeEach
    void setUp() {
        integerStringStringJoinOperation = new LeftJoinOperation<>();
        stringIntegerIntegerJoinOperation = new LeftJoinOperation<>();

        firstDataRow1 = new DataRow<>(1, "First1");
        firstDataRow3 = new DataRow<>(3, "First3");
        firstDataRow6 = new DataRow<>(6, "First6");
        secondDataRow1 = new DataRow<>(1, "Second1");
        secondDataRow3 = new DataRow<>(3, "Second3");

        joinedDataRow1 = new JoinedDataRow<>(1, "First1", "Second1");
        joinedDataRow2 = new JoinedDataRow<>(3, "First3", "Second3");
        joinedDataRow3 = new JoinedDataRow<>(6, "First6", null);
    }

    @Test
    void shouldReturnLeftJoinedRow() {
        DataRow<Integer, String> firstDataRow = new DataRow<>(0, "First");
        DataRow<Integer, String> secondDataRow = new DataRow<>(5, "Second");
        DataRow<Integer, String> secondDataRow2 = new DataRow<>(0, "Second2");
        DataRow<Integer, String> secondDataRow3 = new DataRow<>(1, "Second3");

        Collection<DataRow<Integer, String>> rowList1 = List.of(firstDataRow);
        Collection<DataRow<Integer, String>> rowList2 =
                List.of(secondDataRow, secondDataRow2, secondDataRow3);

        JoinedDataRow<Integer, String, String> joinedDataRow =
                new JoinedDataRow<>(0, "First", "Second2");
        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                List.of(joinedDataRow);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowList1, rowList2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct single item in the collection.");
    }

    @Test
    void shouldReturnSortedLeftJoinedRows() {
        Collection<DataRow<Integer, String>> rowList1 =
                List.of(firstDataRow6, firstDataRow3, firstDataRow1);
        Collection<DataRow<Integer, String>> rowList2 =
                List.of(secondDataRow1, secondDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                List.of(joinedDataRow1, joinedDataRow2, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowList1, rowList2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item list.");
    }

    @Test
    void shouldWorkWithLeftQueueCollection() {
        Queue<DataRow<Integer, String>> rowQueue = new LinkedList<>();
        rowQueue.add(firstDataRow6);
        rowQueue.add(firstDataRow3);
        rowQueue.add(firstDataRow1);
        Collection<DataRow<Integer, String>> rowList =
                List.of(secondDataRow1, secondDataRow3);

        Queue<JoinedDataRow<Integer, String, String>> expectedCollection = new LinkedList<>();
        expectedCollection.add(joinedDataRow1);
        expectedCollection.add(joinedDataRow2);
        expectedCollection.add(joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowQueue, rowList);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item collection.");
    }

    @Test
    void shouldReturnSortedSetOfLeftJoinedRows() {
        Collection<DataRow<Integer, String>> rowSet1 =
                Set.of(firstDataRow1, firstDataRow3, firstDataRow6);
        Collection<DataRow<Integer, String>> rowSet2 = Set.of(secondDataRow1, secondDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                Set.of(joinedDataRow1, joinedDataRow2, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowSet1, rowSet2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item set.");
    }

    @Test
    void shouldReturnSortedSetOfLeftJoinedRowsWithoutNullKey() {
        DataRow<Integer, String> nullKeyDataRow = new DataRow<>(null, "null");
        Collection<DataRow<Integer, String>> rowSet1 =
                Set.of(firstDataRow1, firstDataRow3, firstDataRow6, nullKeyDataRow);
        Collection<DataRow<Integer, String>> rowSet2 = Set.of(secondDataRow1, secondDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                Set.of(joinedDataRow1, joinedDataRow2, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowSet1, rowSet2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item set.");
    }

    @Test
    void shouldReturnJoinedSetOfLeftSetAndRightList() {
        Collection<DataRow<Integer, String>> rowSet1 =
                Set.of(firstDataRow6, firstDataRow3, firstDataRow1);
        Collection<DataRow<Integer, String>> rowSet2 = List.of(secondDataRow1, secondDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                Set.of(joinedDataRow1, joinedDataRow2, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowSet1, rowSet2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item set.");
    }

    @Test
    void shouldReturnJoinedListOfLeftListAndRightSet() {
        Collection<DataRow<Integer, String>> rowSet1 =
                List.of(firstDataRow6, firstDataRow3, firstDataRow1);
        Collection<DataRow<Integer, String>> rowSet2 = Set.of(secondDataRow1, secondDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                List.of(joinedDataRow1, joinedDataRow2, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowSet1, rowSet2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item list.");
    }

    @Test
    void shouldReturnSortedLeftJoinedRowsWithStringKeyAndIntegerValue() {
        DataRow<String, Integer> firstDataRow = new DataRow<>("FK", 11);
        DataRow<String, Integer> firstDataRow2 = new DataRow<>("SK", 21);
        DataRow<String, Integer> firstDataRow3 = new DataRow<>("TK", 31);
        DataRow<String, Integer> secondDataRow = new DataRow<>("FK", 12);
        DataRow<String, Integer> secondDataRow2 = new DataRow<>("SK", 22);

        Collection<DataRow<String, Integer>> rowList1 =
                List.of(firstDataRow, firstDataRow2, firstDataRow3);
        Collection<DataRow<String, Integer>> rowList2 =
                List.of(secondDataRow, secondDataRow2);

        JoinedDataRow<String, Integer, Integer> joinedDataRow =
                new JoinedDataRow<>("FK", 11, 12);
        JoinedDataRow<String, Integer, Integer> joinedDataRow2 =
                new JoinedDataRow<>("SK", 21, 22);
        JoinedDataRow<String, Integer, Integer> joinedDataRow3 =
                new JoinedDataRow<>("TK", 31, null);
        Collection<JoinedDataRow<String, Integer, Integer>> expectedCollection =
                List.of(joinedDataRow, joinedDataRow2, joinedDataRow3);

        Collection<JoinedDataRow<String, Integer, Integer>> joinedCollection =
                stringIntegerIntegerJoinOperation.join(rowList1, rowList2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item list.");
    }

    @Test
    void shouldReturnLeftJoinedRowsWithNullSecondValue() {
        DataRow<Integer, String> firstDataRow = new DataRow<>(1, "First1");
        DataRow<Integer, String> firstDataRow2 = new DataRow<>(2, "First2");
        DataRow<Integer, String> firstDataRow3 = new DataRow<>(3, "First3");

        Collection<DataRow<Integer, String>> rowList1 =
                List.of(firstDataRow, firstDataRow2, firstDataRow3);
        Collection<DataRow<Integer, String>> rowList2 = List.of();

        JoinedDataRow<Integer, String, String> joinedDataRow =
                new JoinedDataRow<>(1, "First1", null);
        JoinedDataRow<Integer, String, String> joinedDataRow2 =
                new JoinedDataRow<>(2, "First2", null);
        JoinedDataRow<Integer, String, String> joinedDataRow3 =
                new JoinedDataRow<>(3, "First3", null);
        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                List.of(joinedDataRow, joinedDataRow2, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowList1, rowList2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item list.");
    }

    @Test
    void shouldReturnLeftJoinedRowsWithoutNullKey() {
        DataRow<Integer, String> firstDataRow2 = new DataRow<>(null, "FirstNull");
        DataRow<Integer, String> secondDataRow2 = new DataRow<>(null, "SecondNull");

        Collection<DataRow<Integer, String>> rowList1 =
                List.of(firstDataRow1, firstDataRow2, firstDataRow3);
        Collection<DataRow<Integer, String>> rowList2 =
                List.of(secondDataRow1, secondDataRow2);

        JoinedDataRow<Integer, String, String> joinedDataRow =
                new JoinedDataRow<>(1, "First1", "Second1");
        JoinedDataRow<Integer, String, String> joinedDataRow3 =
                new JoinedDataRow<>(3, "First3", null);
        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                List.of(joinedDataRow, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowList1, rowList2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item list.");
    }

    @Test
    void shouldReturnEmptyCollection() {
        DataRow<Integer, String> firstDataRow = new DataRow<>(null, "First");
        DataRow<Integer, String> firstDataRow2 = new DataRow<>(null, "FirstNull");
        DataRow<Integer, String> secondDataRow = new DataRow<>(null, "Second");
        DataRow<Integer, String> secondDataRow2 = new DataRow<>(null, "SecondNull");
        DataRow<Integer, String> secondDataRow3 = new DataRow<>(null, "Second3");

        Collection<DataRow<Integer, String>> rowList1 =
                List.of(firstDataRow, firstDataRow2);
        Collection<DataRow<Integer, String>> rowList2 =
                List.of(secondDataRow, secondDataRow2, secondDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowList1, rowList2);
        assertEquals(Collections.EMPTY_LIST, joinedCollection,
                "The method must return an empty collection.");
    }
}