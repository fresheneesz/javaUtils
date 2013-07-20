package com.offers.HAL;

import org.apache.taglibs.standard.lang.jpath.example.Person;
import org.hibernate.*;
import org.hibernate.mapping.Column;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.type.NullableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.util.HibernateUtil;
import com.util.Pair;
import com.util.OfferUtil;

import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.io.Serializable;

public class HAL {
    private static final Logger logger = LoggerFactory.getLogger(HAL.class);
    private static ThreadLocal<List<Session>> openSessions = new ThreadLocal<List<Session>>() {
        @Override public List<Session> initialValue() { // make sure each thread gets a new list
            return new ArrayList<Session>();
        }
    };
    private static ThreadLocal<Map<Session, String>> openSessionInfo = new ThreadLocal<Map<Session,String>>() {    // todo: remove this after figuring out why this sometimes has to clean up hibernate sessions
        @Override public Map<Session,String> initialValue() {
            return new HashMap<Session,String>();
        }
    };

    // readonly, writeable, and readMaster don't get a session until necessary (e.g. until a query is run)

    /**
     * For read only queries (may go to non-master databases).
     * 
     * @return HAL
     */
    public static HAL readonly() {
        return readonly(false);
    }
    /**
     * For read only queries (may go to non-master databases).
     * @param transactional For making multiple queries with the possibility of 
     * rollback.  (NOTE: transactional sessions *must* handle rollback in the
     * case of an exception)
     *
     * @return HAL
     */
    public static HAL readonly(boolean transactional) {
        return getHALSession(HibernateUtil.sessionFactoryForReadOnly, transactional);
    }
    /**
     * For writing (always goes to master database).  Spelled 'writable' in real
     * life.
     *
     * @return HAL
     */
    public static HAL writeable() {
        return writeable(false);
    }

    /**
     * For writing (always goes to master database).  (NOTE: transactional
     * sessions *must* handle rollback in the case of an exception)
     *
     * @return HAL
     */
    public static HAL writeable(boolean transactional) {
        return getHALSession(HibernateUtil.sessionFactoryForWritable, transactional);
    }
    /**
     * For read only.  Always goes to master database - currently the same source
     * as writeable.
     * 
     * @return HAL
     */
    public static HAL readMaster() {
        return writeable(false);
    }
    /**
     * For read only.  Always goes to master database - currently the same source
     * as writeable.  (NOTE: transactional sessions *must* handle rollback in
     * the case of an exception)
     *
     * @return HAL
     */
    public static HAL readMaster(boolean transactional) {
        return getHALSession(HibernateUtil.sessionFactoryForWritable, transactional);
    }

    /**
     * Ensures that all sessions created for a request have been properly 
     * returned to the pool.  Cleans everything up and throws an exception if
     * this isn't true.
     */
    public static void lastResortCleanup(HttpServletRequest req) {
        boolean somethingNeededCleanup = false;
        int leftOverSessions = openSessions.get().size();
        Iterator<Session> i = openSessions.get().iterator();
        while(i.hasNext()) {
            Session s = i.next();
            somethingNeededCleanup = cleanup(s) || somethingNeededCleanup;
            i.remove();
        }

        if(leftOverSessions > 0) {
            String message = "Some session wasn't freed properly";
            if(somethingNeededCleanup)
                message+= " and needed closing";
            message += " - cleaned it up (FIX THIS).";               // if this happens, check uses of HAL.readonly(true) or HAL.writable(true)

            OfferUtil.warn(logger, message, req, null, null);
        }
    }

    private static HAL getHALSession(SessionFactory factory, boolean transactional) {
        HAL h = new HAL();
        h.transactional = transactional;
        h.sessionFactory = factory;
        return h;
    }

    // should only be used in cases where something has gone wrong and the session has not been returned to the pool
    private static boolean cleanup(Session s) {
        if(s!=null && s.isOpen()) {
            boolean rollback = s.getTransaction().isActive();

            String message = "What are you doing, Dave? Closing unclosed session in cleanup";
            if(rollback) message += " and rollingback active transaction";
            logger.warn(message+". "+openSessionInfo.get().get(s));
            openSessionInfo.get().remove(s);

            if(rollback) s.getTransaction().rollback();
            s.close();

            return true;
        } else {
            return false;
        }
    }


    private SessionFactory sessionFactory;
    private Session hibernateSession = null;
    private boolean transactional, raw;
    private String query;
    private List<Pair<String, NullableType>> columns = new ArrayList<Pair<String, NullableType>>();
    private List<Pair<String, Object>> parameters = new ArrayList<Pair<String, Object>>();
    private List<Pair<String, Collection>> listParameters = new ArrayList<Pair<String, Collection>>();
    private Integer limit = null, timeout;
    private Pair<String, LockMode> lockMode = null;

    /*protected void finalize() throws Throwable {
        super.finalize();
        cleanup();
    }*/

    /**
     * Saves an object.  Note that this is *not* the same as a hibernate save.
     * ids for new objects will become available after flush/commit
     */
    public <T> T save(T o) {
        final Session session = getSession(true);

        return runDatabaseAction("saving an object into the database", true, o, new OfferUtil.GeneralRunnable<T, T>(new OfferUtil.Goable<T, T>() {
            public T go(T o) throws Throwable {
                // See the following for save vs persist:
                // http://stackoverflow.com/questions/1069992/jpa-entitymanager-why-use-persist-over-save
                // http://stackoverflow.com/questions/4509086/what-is-the-difference-between-persist-and-save-in-hibernate
                // http://www.stevideter.com/2008/12/07/saveorupdate-versus-save-in-hibernate/
                // https://forum.hibernate.org/viewtopic.php?t=951275
                // http://stackoverflow.com/questions/161224/what-are-the-differences-between-the-different-saving-methods-in-hibernate

                boolean persist = true;
                if(!session.contains(o)) {
                    Serializable id = getId(o);
                    if(id != null && !id.equals(new Long(0))) {                  // does the object exist in the database (if it has an id then it does)
                        persist = false;
                    }
                }

                if(persist) {
                    session.persist(o);
                } else {
                    session.update(o);
                }

                if(!transactional) {
                    commit_internal();
                    free();
                }

                return null;
            }
        }));
    }

    /**
     * Saves an object from an external database (which may have an id that needs to be set).
     * this exists because I can't get hibernate to save an object that *has* an ID (because its from another database) - unbelievable
     */
    public <T> T saveExternal(T o) {
        final Session session = getSession(true);

        return runDatabaseAction("saving an external object into the database", true, o, new OfferUtil.GeneralRunnable<T, T>(new OfferUtil.Goable<T, T>() {
            public T go(T o) throws Throwable {
                Serializable id = getId(o);
                String idProperty = getIdPropertyName(o);
                String idColumnName = getColumnNameForProperty(o, idProperty);
                String tableName = getTableName(o);

                boolean save = true; // set to false if we should do an "insert"
                List results = session.createSQLQuery("select "+idColumnName+" from "+tableName+" where "+idColumnName+"=:id limit 1").setParameter("id", id).list();
                if(results.size() == 0) { // it exists!
                    save = false;
                }

                if(save) {  // to save instead of insert, delete, then insert
                    session.createSQLQuery("delete from "+tableName+" where "+idColumnName+"=:id").setParameter("id", id).executeUpdate();
                }

                session.save(o);
                session.flush();

                Serializable fakeId = getId(o);
                session.createSQLQuery("update "+tableName+" set "+getIdPropertyName(o)+"=:realId where id=:fakeId")
                    .setParameter("realId", id)
                    .setParameter("fakeId", fakeId)
                    .executeUpdate();

                session.evict(o);
                setId(o, id);

                if(!transactional) {
                    commit_internal();
                    free();
                }

                return null;
            }
        }));
    }


    private <T> Serializable getId(T o) {
        return sessionFactory.getClassMetadata(o.getClass()).getIdentifier(o, EntityMode.POJO);
    }
    private <T> void setId(T o, Serializable id) {
        sessionFactory.getClassMetadata(o.getClass()).setIdentifier(o, id, EntityMode.POJO);
    }

    public String getIdPropertyName(Object o) {
        return sessionFactory.getClassMetadata(o.getClass()).getIdentifierPropertyName();
    }
    public String getColumnNameForProperty(Object o, String property) {
        return ((AbstractEntityPersister) sessionFactory.getClassMetadata(o.getClass())).getPropertyColumnNames(property)[0];
    }

    public String getTableName(Object o) {
        ClassMetadata hibernateMetadata = sessionFactory.getClassMetadata(o.getClass());
        AbstractEntityPersister persister = (AbstractEntityPersister) hibernateMetadata;
        return persister.getTableName();
    }

    // deletes an object
    public <T> T delete(T o) {
        final Session session = getSession(true);

        return runDatabaseAction("deleting an object from the database", true, o, new OfferUtil.GeneralRunnable<T, T>(new OfferUtil.Goable<T, T>() {
            public T go(T o) throws Throwable {
                session.delete(o);

                if(!transactional) {
                    commit_internal();
                    free();
                }

                return null;
            }
        }));
    }

    /* saving these functions because the above "save" function may eventually be found to not be sufficient, in which case we may want to consider switching to using merge and persist in different cases

    // does a hibernate merge, which "saves" the object to the database
    // note that for a new object, the passed object will not get any idea - only the *returned* object will have an id set for it
    public <T> T merge(T o) {
        return persavedate(o, false);
    }

    // does a hibernate "persist" on an object - persists the object to the database and tracks changes
    // note that an id will be set on new objects on flush/commit
    // also note that this can't be used on detached entities (use merge instead)
    public <T> void persist(T o) {
        persavedate(o, true);
    }

    private <T> T persavedata(T o, final boolean persist) {
        final Session session = getSession(true);

        return runDatabaseAction("merging objects into the database (like persisting/saving)", true, o, new OfferUtil.GeneralRunnable<T, T>(new OfferUtil.Goable<T, T>() {
            public T go(T o) throws Throwable {
                // See the following for save vs persist:
                // http://stackoverflow.com/questions/1069992/jpa-entitymanager-why-use-persist-over-save
                // http://stackoverflow.com/questions/4509086/what-is-the-difference-between-persist-and-save-in-hibernate
                // http://www.stevideter.com/2008/12/07/saveorupdate-versus-save-in-hibernate/
                // https://forum.hibernate.org/viewtopic.php?t=951275
                // http://stackoverflow.com/questions/161224/what-are-the-differences-between-the-different-saving-methods-in-hibernate
                T result = null;
                if(persist)
                    session.persist(o);
                else
                    result = (T) session.merge(o);


                if(!transactional) {
                    commit_internal();
                    free();
                }

                return result;
            }
        }));
    }*/

    // gets an object by its primary key (via a hibernate get)
    public Object get(Class c, Serializable key) {
        final Session session = getSession(false);
        Object result = session.get(c, key);
        if(!transactional) {
            free();
        }
        return result;
    }

    public HAL query(String query) {
        this.query = query;
        this.raw = false;

        return this;
    }

    // sets the value of a parameter like ":parameter" in the query
    public HAL param(String parameter, Object value) {
        parameters.add(Pair.newPair(parameter, value));
        return this;
    }

    public HAL rawQuery(String query) {
        this.query = query;
        this.raw = true;
        return this;
    }

    /**
     * Adds a column that will be outputed as a result of a rawQuery.
     */
    public HAL addScalar(String name, org.hibernate.type.NullableType dataType) {
        columns.add(Pair.newPair(name, dataType));
        return this;
    }

    // sets the limit
    public HAL limit(int max) {
        limit = max;
        return this;
    }
    // sets the timeout for the query (in seconds)
    public HAL timeout(int seconds) {
        timeout = seconds;
        return this;
    }
    // sets the hibernate lock mode
    public HAL lockMode(String name, LockMode mode) {
        lockMode = Pair.newPair(name, mode);
        return this;
    }

    // returns a list of results
    public List list() {
        return runQuery(false, new OfferUtil.GeneralRunnable<List, Query>(new OfferUtil.Goable<List, Query>() {
            public List go(Query q) throws Throwable {
                return q.list();
            }
        }));
    }


    // returns one object or null if there are no results
    // throws an exception if more than one result is found
    public Object single() {
        return runQuery(false, new OfferUtil.GeneralRunnable<Object, Query>(new OfferUtil.Goable<Object, Query>() {
            public Object go(Query q) throws Throwable {
                return q.uniqueResult();
            }
        }));
    }

    // returns the result object
    // throws an exception if either 0 or more than 1 results are found   (NOTE THAT THIS IS NOT HOW HIBERNATE UNIQUE WORKS)
    public Object unique() {
        Object result = single();
        if(result == null) throw new RuntimeException("No result found for unique");
        return result;
    }

    // returns the number of rows changed
    public int execute() {
        return runQuery(true, new OfferUtil.GeneralRunnable<Integer, Query>(new OfferUtil.Goable<Integer, Query>() {
            public Integer go(Query q) throws Throwable {
                int n = q.executeUpdate();

                if(!transactional) {
                    commit_internal();
                }

                return n;
            }
        }));
    }

    // same thing as commit - can be used for multiple-query readonly sessions where a commit is required, but is funny terminology to use
    public void done() {
        commit();
    }

    // commits transaction and returns the connection back to the pool
    public void commit() {
        runDatabaseAction("commiting a database transaction", true, null, new OfferUtil.GeneralRunnable<Object, Object>(new OfferUtil.Goable<Object, Object>() {
            public List go(Object q) throws Throwable {
                // hibernateSession will be null if nothing has actually been done with the HAL session.
                if (hibernateSession != null) {
                    commit_internal();
                    free();
                }
                return null;
            }
        }));
    }
    public void rollback() {
        hibernateSession.getTransaction().rollback();
    }

    // use this if you need something from the db before committing (e.g. an auto-generated id)
    public void flush() {
        runDatabaseAction("flushing the session", false, null, new OfferUtil.GeneralRunnable<Object, Object>(new OfferUtil.Goable<Object, Object>() {
            public List go(Object q) throws Throwable {
                hibernateSession.flush();
                return null;
            }
        }));
    }

    // commits the transaction
    private void commit_internal() {
        hibernateSession.getTransaction().commit();
    }

    private Session getSession(boolean alwaysNeedsTransaction) {
        if(hibernateSession == null) {
            hibernateSession = sessionFactory.openSession(); // get new session
            if(alwaysNeedsTransaction || transactional) hibernateSession.beginTransaction();
            openSessions.get().add(hibernateSession);
            openSessionInfo.get().put(hibernateSession, this.toString());
        }

        return hibernateSession;
    }

    // returns the connection back to the pool
    private void free() {
        closeSession();
        openSessions.get().remove(hibernateSession);
        openSessionInfo.get().remove(hibernateSession);
        hibernateSession = null;
    }

    private void closeSession() {
        try {
            hibernateSession.close();
        } catch (HibernateException e) {
            logger.warn("Failed to close session: "+ hibernateSession, e);
        }
    }

    // resets in preparation for a new query
    private void reset() {
        query = null;
        columns = new ArrayList<Pair<String, NullableType>>();
        parameters = new ArrayList<Pair<String, Object>>();
        limit = timeout = null;
        lockMode = null;
    }



    // runs the given action
    // optionally resets the query
    // in the case a duplicate entry constraint violation is thrown, does not clean up
    // in the case of an exception, properly cleans up
    private <ReturnType, ParameterType> ReturnType runDatabaseAction(String         actionString,
                                                                     boolean        reset,
                                                                     ParameterType  parameter,
                                                                     OfferUtil.GeneralRunnable<ReturnType, ParameterType> action) {
        try {
            action.setParameter(parameter);
            action.run();
            return action.getResult();

        } catch(Throwable t) {  // handles a database exception properly - by ensuring that transactions are rolled back and sessions are closed

            String message = "Exception "+actionString;
            if( ! transactional) {
                message +=", session has been closed";
                if(hibernateSession != null) {
                    if(hibernateSession.getTransaction().isActive()) {
                        hibernateSession.getTransaction().rollback();
                        message+= " and transaction has been rolled back";
                    }

                    free();
                }
            }

            if(t instanceof HibernateException) {
                OfferUtil.addCause(t, new Exception(message+"."));
                throw (HibernateException) t;
            } else {
                throw OfferUtil.wrapException(message+".", t);
            }

        } finally {
            if(reset) {
                reset();
            }
        }
    }

    /**
     * Runs a query.  Allows for various ways of getting the results and
     * returning results via the action parameter
     */
    public <ReturnType> ReturnType runQuery(boolean requireTransaction, final OfferUtil.GeneralRunnable<ReturnType, Query> action) {
        Session session = getSession(requireTransaction);  // get new session

        Query q;
        if(raw) {
            q = session.createSQLQuery(query);
            for(Pair<String, NullableType> column : columns) {
                ((SQLQuery)q).addScalar(column.getFirst(), column.getSecond());
            }
        } else {
            q = session.createQuery(query);
        }

        for(Pair<String, Object> parameter : parameters) {
            String name = parameter.getFirst();
            Object param = parameter.getSecond();
            if(param instanceof Collection) {
                q.setParameterList(name, (Collection) param);
            } else {
                q.setParameter(name, param);
            }
        }

        if(limit != null) q.setMaxResults(limit);
        if(timeout != null) q.setTimeout(timeout);
        if(lockMode != null) q.setLockMode(lockMode.getFirst(), lockMode.getSecond());

        ReturnType result = runDatabaseAction("running query", true, q, new OfferUtil.GeneralRunnable<ReturnType, Query>(new OfferUtil.Goable<ReturnType, Query>() {
            public ReturnType go(Query q) throws Throwable {
                action.setParameter(q);
                action.run();
                return action.getResult();
            }
        }));

        if(!transactional) {
            free();
        }

        return result;
    }

    @Override
    public String toString() {
        return "HAL{transactional="+transactional+'}';
    }
}
