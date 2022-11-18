package aa.intellias.testtask.operation.impl;

import aa.intellias.testtask.collection.DataRow;
import aa.intellias.testtask.collection.JoinedDataRow;
import aa.intellias.testtask.operation.JoinOperation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class RightJoinOperation<K extends Comparable<K>, V1, V2>
        implements JoinOperation<DataRow<K, V1>, DataRow<K, V2>, JoinedDataRow<K, V1, V2>> {
    @Override
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection,
                                                     Collection<DataRow<K, V2>> rightCollection) {
        Collection<JoinedDataRow<K, V1, V2>> rightJoinedCollection = checkInstance(rightCollection);
        for (DataRow<K, V2> rightRow : rightCollection) {
            if (rightRow.getKey() == null) {
                continue;
            }
            JoinedDataRow<K, V1, V2> rightJoinedDataRow = new JoinedDataRow<>();
            rightJoinedDataRow.setKey(rightRow.getKey());
            rightJoinedDataRow.setRightValue(rightRow.getValue());
            Optional<DataRow<K, V1>> leftRowOptional = leftCollection.stream()
                    .filter(row -> row.getKey() != null)
                    .filter(row -> row.getKey().equals(rightRow.getKey()))
                    .findFirst();
            leftRowOptional.ifPresent(row -> rightJoinedDataRow.setLeftValue(row.getValue()));
            rightJoinedCollection.add(rightJoinedDataRow);
        }
        return rightJoinedCollection instanceof Set
                ? rightJoinedCollection : rightJoinedCollection.stream()
                .filter(row -> row.getKey() != null)
                .sorted()
                .toList();
    }

    private Collection<JoinedDataRow<K, V1, V2>> checkInstance(
            Collection<DataRow<K, V2>> rightCollection) {
        Collection<JoinedDataRow<K, V1, V2>> rightJoinedCollection;
        if (rightCollection instanceof Set) {
            rightJoinedCollection = new TreeSet<>();
        } else {
            rightJoinedCollection = new ArrayList<>();
        }
        return rightJoinedCollection;
    }
}
