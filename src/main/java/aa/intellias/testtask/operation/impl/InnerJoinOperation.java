package aa.intellias.testtask.operation.impl;

import aa.intellias.testtask.collection.DataRow;
import aa.intellias.testtask.collection.JoinedDataRow;
import aa.intellias.testtask.operation.JoinOperation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class InnerJoinOperation<K extends Comparable<K>, V1, V2>
        implements JoinOperation<DataRow<K, V1>, DataRow<K, V2>, JoinedDataRow<K, V1, V2>> {

    @Override
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection,
                                                     Collection<DataRow<K, V2>> rightCollection) {
        Collection<JoinedDataRow<K, V1, V2>> joinedCollection =
                checkInstance(leftCollection, rightCollection);
        for (DataRow<K, V1> leftRow : leftCollection) {
            Optional<DataRow<K, V2>> rightRowOptional = rightCollection.stream()
                    .filter(row -> row != null && row.getKey() != null)
                    .filter(row -> row.getKey().equals(leftRow.getKey()))
                    .findFirst();
            if (rightRowOptional.isPresent()) {
                DataRow<K, V2> rightDataRow = rightRowOptional.get();
                JoinedDataRow<K, V1, V2> joinedDataRow = new JoinedDataRow<>(leftRow.getKey(),
                        leftRow.getValue(), rightDataRow.getValue());
                joinedCollection.add(joinedDataRow);
            }
        }
        return joinedCollection instanceof Set
                ? joinedCollection : joinedCollection.stream()
                .filter(row -> row.getKey() != null)
                .sorted()
                .toList();
    }

    private Collection<JoinedDataRow<K, V1, V2>> checkInstance(
            Collection<DataRow<K, V1>> leftCollection, Collection<DataRow<K, V2>> rightCollection) {
        Collection<JoinedDataRow<K, V1, V2>> joinedCollection;
        if (leftCollection instanceof Set || rightCollection instanceof Set) {
            joinedCollection = new TreeSet<>();
        } else {
            joinedCollection = new ArrayList<>();
        }
        return joinedCollection;
    }
}
