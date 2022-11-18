package aa.intellias.testtask.operation.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import aa.intellias.testtask.collection.DataRow;
import aa.intellias.testtask.collection.JoinedDataRow;
import aa.intellias.testtask.operation.JoinOperation;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InnerJoinOperationTest {
    private JoinOperation<DataRow<Integer, String>, DataRow<Integer, String>,
            JoinedDataRow<Integer, String, String>> integerStringStringJoinOperation;
    private JoinOperation<DataRow<String, String>, DataRow<String, String>,
            JoinedDataRow<String, String, String>> stringStringStringJoinOperation;
    private JoinOperation<DataRow<String, Integer>, DataRow<String, Integer>,
            JoinedDataRow<String, Integer, Integer>> stringIntegerIntegerJoinOperation;
    private JoinOperation<DataRow<String, String>, DataRow<String, Integer>,
            JoinedDataRow<String, String, Integer>> stringStringIntegerJoinOperation;
    private DataRow<Integer, String> firstDataRow1;
    private DataRow<Integer, String> firstDataRow2;
    private DataRow<Integer, String> firstDataRow3;
    private DataRow<Integer, String> secondDataRow1;
    private DataRow<Integer, String> secondDataRow2;
    private DataRow<Integer, String> secondDataRow3;
    private JoinedDataRow<Integer, String, String> joinedDataRow1;
    private JoinedDataRow<Integer, String, String> joinedDataRow2;
    private JoinedDataRow<Integer, String, String> joinedDataRow3;

    @BeforeEach
    void setUp() {
        integerStringStringJoinOperation = new InnerJoinOperation<>();
        stringStringStringJoinOperation = new InnerJoinOperation<>();
        stringIntegerIntegerJoinOperation = new InnerJoinOperation<>();
        stringStringIntegerJoinOperation = new InnerJoinOperation<>();

        firstDataRow1 = new DataRow<>(1, "First1");
        firstDataRow2 = new DataRow<>(2, "First2");
        firstDataRow3 = new DataRow<>(3, "First3");
        secondDataRow1 = new DataRow<>(1, "Second1");
        secondDataRow2 = new DataRow<>(2, "Second2");
        secondDataRow3 = new DataRow<>(3, "Second3");

        joinedDataRow1 = new JoinedDataRow<>(1, "First1", "Second1");
        joinedDataRow2 = new JoinedDataRow<>(2, "First2", "Second2");
        joinedDataRow3 = new JoinedDataRow<>(3, "First3", "Second3");
    }

    @Test
    void shouldReturnInnerJoinedRow() {
        Collection<DataRow<Integer, String>> rowList1 = List.of(firstDataRow1);
        Collection<DataRow<Integer, String>> rowList2 =
                List.of(secondDataRow1, secondDataRow2, secondDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                List.of(joinedDataRow1);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowList1, rowList2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct single item in the collection.");
    }

    @Test
    void shouldReturnInnerJoinedRowWithStringKey() {
        DataRow<String, String> firstDataRow = new DataRow<>("special key", "First");
        DataRow<String, String> secondDataRow = new DataRow<>("common key", "Second");
        DataRow<String, String> secondDataRow2 = new DataRow<>("special key", "Second2");

        Collection<DataRow<String, String>> rowList1 = List.of(firstDataRow);
        Collection<DataRow<String, String>> rowList2 = List.of(secondDataRow, secondDataRow2);

        JoinedDataRow<String, String, String> joinedDataRow =
                new JoinedDataRow<>("special key", "First", "Second2");
        Collection<JoinedDataRow<String, String, String>> expectedCollection =
                List.of(joinedDataRow);

        Collection<JoinedDataRow<String, String, String>> joinedCollection =
                stringStringStringJoinOperation.join(rowList1, rowList2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct single item in the collection.");
    }

    @Test
    void shouldReturnInnerJoinedRowWithStringKeyAndIntegerValue() {
        DataRow<String, Integer> firstDataRow = new DataRow<>("FK", 1);
        DataRow<String, Integer> secondDataRow = new DataRow<>("SK", 2);
        DataRow<String, Integer> secondDataRow2 = new DataRow<>("FK", 3);

        Collection<DataRow<String, Integer>> rowList1 = List.of(firstDataRow);
        Collection<DataRow<String, Integer>> rowList2 = List.of(secondDataRow, secondDataRow2);

        JoinedDataRow<String, Integer, Integer> joinedDataRow =
                new JoinedDataRow<>("FK", 1, 3);
        Collection<JoinedDataRow<String, Integer, Integer>> expectedCollection =
                List.of(joinedDataRow);

        Collection<JoinedDataRow<String, Integer, Integer>> joinedCollection =
                stringIntegerIntegerJoinOperation.join(rowList1, rowList2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct single item in the collection.");
    }

    @Test
    void shouldReturnInnerJoinedRowWithStringKeyAndStringLeftValueAndIntegerRightValue() {
        DataRow<String, String> firstDataRow = new DataRow<>("SK", "Seven");
        DataRow<String, Integer> secondDataRow = new DataRow<>("FK", 5);
        DataRow<String, Integer> secondDataRow2 = new DataRow<>("SK", 7);

        Collection<DataRow<String, String>> rowList1 = List.of(firstDataRow);
        Collection<DataRow<String, Integer>> rowList2 = List.of(secondDataRow, secondDataRow2);

        JoinedDataRow<String, String, Integer> joinedDataRow =
                new JoinedDataRow<>("SK", "Seven", 7);
        Collection<JoinedDataRow<String, String, Integer>> expectedCollection =
                List.of(joinedDataRow);

        Collection<JoinedDataRow<String, String, Integer>> joinedCollection =
                stringStringIntegerJoinOperation.join(rowList1, rowList2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct single item in the collection.");
    }

    @Test
    void shouldReturnInnerJoinedRows() {
        Collection<DataRow<Integer, String>> rowList1 =
                List.of(firstDataRow1, firstDataRow2, firstDataRow3);
        Collection<DataRow<Integer, String>> rowList2 =
                List.of(secondDataRow1, secondDataRow2, secondDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                List.of(joinedDataRow1, joinedDataRow2, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowList1, rowList2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct number of items in the collection.");
    }

    @Test
    void shouldReturnSortedInnerJoinedRows() {
        Collection<DataRow<Integer, String>> rowList1 =
                List.of(firstDataRow3, firstDataRow1, firstDataRow2);
        Collection<DataRow<Integer, String>> rowList2 =
                List.of(secondDataRow2, secondDataRow3, secondDataRow1);

        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                List.of(joinedDataRow1, joinedDataRow2, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowList1, rowList2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct number of items in the collection.");
    }

    @Test
    void shouldReturnSortedSetOfRightJoinedRows() {
        Collection<DataRow<Integer, String>> rowSet1 = Set.of(firstDataRow3, firstDataRow1);
        Collection<DataRow<Integer, String>> rowSet2 =
                Set.of(secondDataRow2, secondDataRow1, secondDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                Set.of(joinedDataRow1, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowSet1, rowSet2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item set.");
    }

    @Test
    void shouldReturnSortedSetOfRightJoinedRowsWithoutNullKey() {
        DataRow<Integer, String> nullKeyDataRow = new DataRow<>(null, "null");
        Collection<DataRow<Integer, String>> rowSet1 =
                Set.of(firstDataRow1, firstDataRow3);
        Collection<DataRow<Integer, String>> rowSet2 =
                Set.of(secondDataRow1, secondDataRow2, secondDataRow3, nullKeyDataRow);

        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                Set.of(joinedDataRow1, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowSet1, rowSet2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item set.");
    }

    @Test
    void shouldReturnJoinedSetOfRightListAndRightSet() {
        Collection<DataRow<Integer, String>> rowSet1 =
                List.of(firstDataRow3, firstDataRow1);
        Collection<DataRow<Integer, String>> rowSet2 =
                Set.of(secondDataRow2, secondDataRow1, secondDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                Set.of(joinedDataRow1, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowSet1, rowSet2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item set.");
    }

    @Test
    void shouldReturnJoinedSetOfRightSetAndRightList() {
        Collection<DataRow<Integer, String>> rowSet1 =
                Set.of(firstDataRow3, firstDataRow1);
        Collection<DataRow<Integer, String>> rowSet2 =
                List.of(secondDataRow2, secondDataRow1, secondDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> expectedCollection =
                Set.of(joinedDataRow1, joinedDataRow3);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowSet1, rowSet2);
        assertEquals(expectedCollection, joinedCollection,
                "The method must return a correct item set.");
    }

    @Test
    void shouldReturnEmptyCollection() {
        DataRow<Integer, String> secondDataRow9 = new DataRow<>(10, "Second9");

        Collection<DataRow<Integer, String>> rowList1 =
                List.of(firstDataRow1, firstDataRow2, firstDataRow3);
        Collection<DataRow<Integer, String>> rowList2 = List.of(secondDataRow9);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowList1, rowList2);
        assertEquals(Collections.EMPTY_LIST, joinedCollection,
                "The method must return an empty collection.");
    }

    @Test
    void shouldReturnEmptyCollectionCauseOfNullKeys() {
        DataRow<Integer, String> firstDataRow = new DataRow<>(null, "FirstNull");
        DataRow<Integer, String> secondDataRow9 = new DataRow<>(null, "SecondNull");

        Collection<DataRow<Integer, String>> rowList1 =
                List.of(firstDataRow, firstDataRow2, firstDataRow3);
        Collection<DataRow<Integer, String>> rowList2 = List.of(secondDataRow9);

        Collection<JoinedDataRow<Integer, String, String>> joinedCollection =
                integerStringStringJoinOperation.join(rowList1, rowList2);
        assertEquals(Collections.EMPTY_LIST, joinedCollection,
                "The method must return an empty collection.");
    }
}