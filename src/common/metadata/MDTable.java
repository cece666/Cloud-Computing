package common.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.stream.Collectors;

import common.hash.Hash;

/**
 * 
 * This class generate MDTable by assembling MDEntries, and offers methods for
 * MDTable operations.
 * 
 * @author uy Ha
 *
 */
public class MDTable implements Iterable<MDEntry> {
    private final ArrayList<MDEntry> entries;
    private final Random random = new Random();

    public MDTable(MDEntry[] entries) {
	this(Arrays.asList(entries));
    }

    public MDTable(List<MDEntry> entries) {
	this.entries = new ArrayList<>();
	this.entries.addAll(entries);
	this.entries.sort(MDEntry::hashIndexComparator);
    }

    /**
     * convert MDTable to String value
     * 
     * @return
     */
    public String toMessageValue() {
	return entries.stream().map(MDEntry::valueString).collect(Collectors.joining("\n"));
    }

    /**
     * pop n random values from MDTable
     * 
     * @param n
     * @return
     * @throws IllegalArgumentException
     */
    public MDTable popNode(int n) throws IllegalArgumentException {
	if (n > entries.size())
	    throw new IllegalArgumentException("Cannot pop more than the available nodes");

	ArrayList<MDEntry> poppedNodes = new ArrayList<>();

	for (int i = 0; i < n; i++) {
	    poppedNodes.add(entries.remove(random.nextInt(entries.size())));
	}

	entries.sort(MDEntry::hashIndexComparator);
	return new MDTable(poppedNodes);
    }

    /**
     * merge two MDTables
     * 
     * @param table
     */
    public void mergeTable(MDTable table) {
	entries.addAll(table.entries);
	entries.sort(MDEntry::hashIndexComparator);
    }

    /**
     * clear MDTable
     */
    public void clear() {
	entries.clear();
    }

    /**
     * add one MDEntry to MDTable
     * 
     * @param entry
     */
    public void addEntry(MDEntry entry) {
	entries.add(entry);
	entries.sort(MDEntry::hashIndexComparator);
    }

    /**
     * remove one MDEntry from MDTable
     * 
     * @param entry
     */
    public void removeEntry(MDEntry entry) {
	entries.remove(entry);
	entries.sort(MDEntry::hashIndexComparator);
    }

    public void removeEntries(Collection<MDEntry> entries) {
	this.entries.removeAll(entries);
	this.entries.sort(MDEntry::hashIndexComparator);
    }

    public void addEntries(Collection<MDEntry> entries) {
	this.entries.addAll(entries);
	this.entries.sort(MDEntry::hashIndexComparator);
    }

    /**
     * pop one random MDEntry from MDTable
     * 
     * @return popped MDEntry
     */
    public MDEntry[] popNode() {
	if (entries.size() == 0)
	    throw new IllegalArgumentException("Table does not have any entry");
	int popIndex = random.nextInt(entries.size());
	MDEntry predecessor = getPredecessor(entries.get(popIndex));
	MDEntry successor = getSuccessor(entries.get(popIndex));

	MDEntry result = entries.remove(popIndex);
	entries.sort(MDEntry::hashIndexComparator);
	return new MDEntry[] { predecessor, result, successor };
    }

    /**
     * search for MDEntry in MDTable
     * 
     * @param entry
     * @return message of MDEntry in MDTable
     */
    public MDEntry getPredecessor(MDEntry entry) {
	int searchResult = Collections.binarySearch(entries, entry, MDEntry::hashIndexComparator);

	if (searchResult < 0)
	    return null;
	if (searchResult == 0)
	    return entries.get(entries.size() - 1);

	return entries.get(searchResult - 1);
    }

    /**
     * search for MDEntry in MDTable
     * 
     * @param entry
     * @return message of MDEntry in MDTable
     */
    public MDEntry getSuccessor(MDEntry entry) {
	int searchResult = Collections.binarySearch(entries, entry, MDEntry::hashIndexComparator);

	if (searchResult < 0)
	    return null;
	if (searchResult == entries.size() - 1)
	    return entries.get(0);

	return entries.get(searchResult + 1);
    }

    /**
     * Estimate whether mdtable have specific entry
     * 
     * @param entry
     * @return Boolean
     */
    public boolean contains(MDEntry entry) {
	return Collections.binarySearch(entries, entry, MDEntry::hashIndexComparator) >= 0;
    }

    /**
     * build mdTable by hashIndex
     * 
     * @param hashIndex
     * @return
     */
    public boolean contains(byte[] hashIndex) {
	return contains(MDEntry.fakeEntry(hashIndex));
    }

    /**
     * build mdTable by byteString
     * 
     * @param byteString
     * @return
     */
    public boolean contains(String byteString) {
	return contains(MDEntry.fakeEntry(byteString.getBytes()));
    }

    /**
     * This gives the destination of which server the message of the given key
     * should be sent to
     * 
     * @param key key of the pair
     * @return the MDEntry of the destination server
     */
    public MDEntry routeMessage(String key) {
	byte[] hashedKey = Hash.hash(key);

	int searchResult = Collections.binarySearch(entries, MDEntry.fakeEntry(hashedKey),
		MDEntry::hashIndexComparator);

	// Taking advantage of the behavior of Arrays.binarySearch
	int entryIndex = searchResult;

	if (entryIndex < 0) {
	    entryIndex += 1;
	    entryIndex *= -1;

	    if (entryIndex == entries.size())
		entryIndex = 0;
	}

	return entries.get(entryIndex);
    }

    /**
     * build MDTable by reading from Config file.
     * 
     * @param file
     * @return MDTable
     * @throws IOException
     */
    public static MDTable fromConfigFile(File file) throws IOException {
	try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
	    String line;
	    ArrayList<MDEntry> entries = new ArrayList<>(20);
	    while ((line = reader.readLine()) != null) {
		entries.add(MDEntry.fromConfigString(line));
	    }
	    return new MDTable(new ArrayList<MDEntry>(entries));
	}
    }

    /**
     * Convert String value to MDTable
     * 
     * @param value
     * @return MDTable
     */
    public static MDTable fromMessageValue(String value) {
	ArrayList<MDEntry> entries = new ArrayList<>(20);
	for (String entryString : value.split("\\r?\\n")) {
	    entries.add(MDEntry.fromConfigString(entryString));
	}
	return new MDTable(new ArrayList<MDEntry>(entries));
    }

    public Queue<MDEntry> getPingedTargets(MDEntry server) {
	int index = Collections.binarySearch(entries, server, MDEntry::hashIndexComparator);

	if (index < 0) {
	    return new LinkedList<MDEntry>();
	}
	int first = (2 * index + 1) % entries.size();
	int second = (2 * index + 2) % entries.size();
	Queue<MDEntry> result = new LinkedList<MDEntry>();
	if (first != index)
	    result.add(entries.get(first));
	if (second != index)
	    result.add(entries.get(second));
	return result;
    }

    @Override
    public Iterator<MDEntry> iterator() {
	return entries.iterator();
    }

    public void popFailureNode(MDEntry node) {
	Iterator<MDEntry> it = entries.iterator();
	while (it.hasNext()) {
	    MDEntry mdEntry = it.next();
	    if (mdEntry.hashIndex == node.hashIndex) {
		it.remove();
	    }
	}
    }
}
