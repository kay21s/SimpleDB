package simpledb;

import java.io.*;
import java.util.*;

import simpledb.TupleDesc.TDItem;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File _file;
    TupleDesc _td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        _file = f;
        _td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return _file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return _file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return _td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        byte[] buf = new byte[BufferPool.getPageSize()];
        FileInputStream fs = null;
        HeapPage p = null;
        
        try {
            fs = new FileInputStream(_file);
            fs.skip(pid.pageNumber() * BufferPool.getPageSize());
            fs.read(buf);
            fs.close();
            p = new HeapPage((HeapPageId)pid, buf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return p;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(_file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    class HeapFileIterator implements DbFileIterator {
        private HeapFile        _hf;
        private TransactionId   _tid;
        private int             _tableid;
        private int             _pageno;
        private Iterator<Tuple> _pageTuples;

        HeapFileIterator(HeapFile hf, TransactionId tid) {
            _hf         = hf;
            _tid        = tid;
            _tableid    = hf.getId();
            _pageno     = 0;
        }

        public void open() throws DbException,
                                  TransactionAbortedException
        {
            _pageTuples = openInternal();
        }

        private Iterator<Tuple> openInternal() throws DbException,
                                                      TransactionAbortedException
        {
            HeapPageId pid = new HeapPageId(_tableid, _pageno);
            HeapPage p = (HeapPage)Database.getBufferPool()
                            .getPage(_tid, pid, Permissions.READ_ONLY);
            return p.iterator();
        }

        public boolean hasNext() throws DbException,
                        TransactionAbortedException
        {
            if (_pageTuples == null) {
                return false;
            } else if (_pageTuples.hasNext()) {
                return true;
            } else {
                _pageno++;
                if (_pageno < _hf.numPages()) {
                    _pageTuples = openInternal();
                    return _pageTuples.hasNext();
                } else {
                    return false;
                }
            }
        }

        public Tuple next() throws DbException,
                                   TransactionAbortedException,
                                   NoSuchElementException
        {
            if (hasNext())
                return _pageTuples.next();
            else
                throw new NoSuchElementException();
        }

        public void rewind() throws DbException,
                                    TransactionAbortedException
        {
            _pageno = 0;
            _pageTuples = openInternal();
        }

        public void close() {
            _pageTuples = null;
        }
    }

}

