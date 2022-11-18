package aa.intellias.testtask.operation.impl;

import aa.intellias.testtask.collection.DataRow;
import aa.intellias.testtask.collection.JoinedDataRow;
import aa.intellias.testtask.operation.JoinOperation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class LeftJoinOperation<K extends Comparable<K>, V1, V2>
        implements JoinOperation<DataRow<K, V1>, DataRow<K, V2>, JoinedDataRow<K, V1, V2>> {
    @Override
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection,
                                                     Collection<DataRow<K, V2>> rightCollection) {
        Collection<JoinedDataRow<K, V1, V2>> leftJoinedCollection = checkInstance(leftCollection);
        for (DataRow<K, V1> leftRow : leftCollection) {
            if (leftRow.getKey() == null) {
                continue;
            }
            JoinedDataRow<K, V1, V2> leftJoinedDataRow = new JoinedDataRow<>();
            leftJoinedDataRow.setKey(leftRow.getKey());
            leftJoinedDataRow.setLeftValue(leftRow.getValue());
            Optional<DataRow<K, V2>> rightRowOptional = rightCollection.stream()
                    .filter(row -> row.getKey() != null)
                    .filter(row -> row.getKey().equals(leftRow.getKey()))
                    .findFirst();
            rightRowOptional.ifPresent(row -> leftJoinedDataRow.setRightValue(row.getValue()));
            leftJoinedCollection.add(leftJoinedDataRow);
        }
        return leftJoinedCollection instanceof Set
                ? leftJoinedCollection : leftJoinedCollection.stream()
                .filter(row -> row.getKey() != null)
                .sorted()
                .toList();
    }

    private Collection<JoinedDataRow<K, V1, V2>> checkInstance(
            Collection<DataRow<K, V1>> leftCollection) {
        Collection<JoinedDataRow<K, V1, V2>> leftJoinedDataRowCollection;
        if (leftCollection instanceof Set) {
            leftJoinedDataRowCollection = new TreeSet<>();
        } else {
            leftJoinedDataRowCollection = new ArrayList<>();
        }
        return leftJoinedDataRowCollection;
    }
}
