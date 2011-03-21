(module
 pandora-freetds
 *
 (import scheme chicken data-structures srfi-13)

 (use prometheus pandora freetds lru-cache uri-generic matchable)

 (define-object *freetds-datastore* (*sql-datastore*)
   (db set-db! #f))

 (define-method (*freetds-datastore* 'clone self resend uri)
   ;; HACK: assuming for now that we have everything
   (let ((host (uri-host uri))
         (port (uri-port uri))
         (username (uri-username uri))
         (password (uri-password uri))
         (path (uri-path uri)))
     (let ((clone (resend #f 'clone)))
       (let ((db (call-with-context
                  (lambda (context)
                    ;; we should probably get rid of the damn context by
                    ;; abstracting it into the connection object
                    (clone 'add-value-slot! 'context 'set-context! context)
                    (make-connection context
                                     (format "~a:~a" host port)
                                     username
                                     password
                                     (cadr path)))))
             (cache (make-lru-cache 64 equal?)))
         (clone 'set-db! db)
         clone))))

 (define-method (*freetds-datastore* 'disconnect! self resend)
   (let ((db (self 'db))
         (cache (self 'cache)))
     (self 'set-db! #f)
     (self 'set-cache! #f)
     (lru-cache-flush! cache)
     (connection-close! db)))

 (define-method (*freetds-datastore* 'table self resend name)
   (let ((primary-keys
          (self 'execute
                (list (format "SELECT [name] FROM syscolumns WHERE [id] IN
  (SELECT [id] FROM sysobjects WHERE [name] = '~a') AND colid IN
    (SELECT SIK.colid FROM sysindexkeys SIK
       JOIN sysobjects SO ON SIK.[id] = SO.[id] WHERE
       SIK.indid = 1 AND SO.[name] = '~a')" 
                              name name))))
         (column-names
          (map (match-lambda ((catalog
                               schema
                               name
                               column-name
                               ordinal
                               default
                               null?
                               data-type
                               max-length
                               octet-length
                               precision
                               precision-radix
                               scale
                               datetime-precision
                               character-set
                               character-set-schema
                               character-set-name
                               collation-catalog
                               collation-scheme
                               collation-name
                               domain-catalog
                               domain-schema
                               domain-name)
                              column-name))
               (self 'execute
                     (list (format "SELECT * FROM information_schema.columns WHERE
                                      table_name = '~a'"
                                   name))))))
     (let ((table (resend #f 'table name)))
       (table 'set-primary-key-clauses! primary-keys)
       (for-each (lambda (column-name)
                   (let ((name (string-translate
                                (string-downcase column-name) "_" "-")))
                     (let ((getter (string->symbol name))
                           (setter (string->symbol
                                    (string-append "set-" name "!"))))
                       (table 'add-column-slots!
                              getter
                              setter
                              column-name))))
                 column-names) 
       table)))

 (define-method (*freetds-datastore* 'execute
                                     self
                                     resend
                                     sql
                                     #!optional
                                     (params '()))
   (let replace ((sql sql)
                 (replaced-sql '())
                 (params params))
     (if (null? sql)
         (begin
           (call-with-result-set
            (self 'db)
            (string-join (reverse replaced-sql))
            (cut result-values
                 (self 'context)
                 (self 'db)
                 <>)))        
         (let* ((fragment (car sql))
                (parameter? (eq? fragment '?)))
           (replace (cdr sql)
                    (cons (if parameter?
                              ;; HACK: this is dangerous; implement
                              ;; parameters or escape this.
                              (->string (car params))
                              fragment)
                          replaced-sql)
                    (if parameter? (cdr params) params))))))

 (define-method (*freetds-datastore* 'fold
                                     self
                                     resend
                                     proc
                                     init
                                     sql
                                     #!optional
                                     (params '()))
   (self 'execute sql params))

 (define-method (*freetds-datastore* 'name->column-clause self resend name)
   (list (->string name)))

 (*sql-datastore* 'add-connection-prototype! 'freetds *freetds-datastore*))
