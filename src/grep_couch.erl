-module(grep_couch).
-export([main/1]).

-define(MB       , 1 * 1024 * 1024).
-define(READ_SIZE, 1 * 1024 * 1024).
%-define(READ_SIZE, 1 * 1024).

% Look for 1-tuples where the first element is a list with fewer than 65k elements.
%-define(T2B_MAGIC, <<131, 104, 1, 108, 0, 0>>).
%-define(MAGIC_LEN, 6).

% Look for lists with fewer than 65k elements.
%-define(T2B_MAGIC, <<131, 108, 0, 0>>).
%-define(MAGIC_LEN, 4).

% Look for 2-tuples where the first element is an atom.
%-define(T2B_MAGIC, <<131, 104, 2, 100>>).
%-define(MAGIC_LEN, 4).

% Look for anything.
%-define(T2B_MAGIC, <<131>>).
%-define(MAGIC_LEN, 1).

% Looking for 2-tuples where the first element is a 1-tuple.
%-define(T2B_MAGIC, <<131, 104, 2, 104, 1>>).
%-define(MAGIC_LEN, 5).

% Look for any 2-tuple.
-define(T2B_MAGIC, <<131, 104, 2>>).
-define(MAGIC_LEN, 3).


puts(Str)
    -> puts(Str, [])
    .

puts(Str, Args)
    -> case os:getenv("verbose")
        of "true"
            -> io:format(Str, Args)
        ; _
            -> ok
        end
    .

usage()
    -> io:format("usage: grep_couchdb /path/to/some/file\n")
    , halt(1)
    .

main([])
    -> usage()
    ;

main(Args=[Path | _Rest]) when is_list(Args)
    -> puts("Scanning ~p\n", [Path])
    , {ok, File} = file:open(Path, [read, raw, binary, {read_ahead, 4 * ?MB}])
    , LogTerm = case os:getenv("terms")
        of false
            -> fun(_Term) -> ok end
        ; TermFilename
            -> {ok, TermFile} = file:open(TermFilename, [write, raw, delayed_write])
            , fun(Term)
                -> file:write(TermFile, io_lib:format("~w.\n", [Term]))
                , ok
            end
        end
    , json_scan(File, LogTerm)
    , halt(0)
    ;

main(_)
    -> usage()
    .

json_scan(File, LogTerm)
    -> json_scan(File, LogTerm, 0, <<"">>, false)
    .

json_scan(File, LogTerm, Offset, Pending, Eof)
    -> PendingSize = size(Pending)
    , CurrentPos = Offset - PendingSize
    , puts("Pos=~p PendingSize=~p\n", [CurrentPos, PendingSize])

    , Result = find_terms(Pending, Eof, LogTerm)
    , puts(" Result: ~p\n", [Result])

    , case Result
        of {error, Reason}
            -> puts("ERROR: ~p\n", [Reason])
            , exit({json_scan_error, Reason})
        ; {ok, Consumed}
            -> Remainder = binary:part(Pending, {Consumed, PendingSize - Consumed})
            , case Eof
                of true
                    % find_terms() should have consumed the entire file.
                    -> case Remainder
                        of <<"">>
                            -> puts("Done.\n")
                        ; _
                            -> exit({bad_finish, Remainder})
                        end
                ; false
                    % find_terms() needs more data.
                    -> case file:read(File, ?READ_SIZE)
                        of {error, Reason}
                            -> puts("ERROR reading: ~p\n", [Reason])
                            , exit({file_read_error, Reason})
                        ; eof
                            -> json_scan(File, LogTerm, Offset + Consumed, Remainder, true)
                        ; {ok, Data}
                            -> json_scan(File, LogTerm, Offset + Consumed + size(Data), <<Remainder/binary, Data/binary>>, false)
                        end
                end
        end
    .

find_terms(Data, Eof, Log)
    -> find_terms(Data, 0, Eof, Log)
    .

find_terms(Data, Sofar, Eof, Log)
    -> DataSize = size(Data)
    , Match = binary:match(Data, ?T2B_MAGIC)
    , case Match
        of nomatch
            % This is all the data so indicate that all the data has been consumed.
            -> {ok, DataSize + Sofar}
        ; {Offset, ?MAGIC_LEN}
            -> CandidateLength = DataSize - Offset
            , case (CandidateLength >= ?READ_SIZE orelse Eof =:= true)
                of true
                    % One way or the other, try to get data from the candidate.
                    -> Candidate = binary:part(Data, {Offset, CandidateLength})
                    , try binary_to_term(Candidate)
                        %of Term when is_list(Term) =:= false
                        %    % Should never happen since the search was for a tuple.
                        %    -> io:format("ERROR: Bad term found: ~p\n", [Term])
                        %; Term when is_list(Term)
                        of Term
                            -> found_term(Term, Log)
                            % Unfortunately, the only thing I know to do at
                            % this point is *re-convert* back to binary to
                            % see how long it was.
                            , TermLength = size(term_to_binary(Term))
                            , Remainder = binary:part(Candidate, {TermLength, CandidateLength - TermLength})
                            , find_terms(Remainder, Sofar + Offset + TermLength, Eof, Log)
                    catch error:badarg
                        % Despite the magic string being found, a term is not here. Skip over the magic bytes.
                        % Probably not enough data collected.
                        -> puts("Sneaky data (~p):\n~p\nEND SNEAKY\n", [CandidateLength, Candidate])
                        , Log({suspicious, CandidateLength, Candidate})
                        , Remainder = binary:part(Candidate, {?MAGIC_LEN, CandidateLength - ?MAGIC_LEN})
                        , find_terms(Remainder, Sofar + Offset + ?MAGIC_LEN, Eof, Log)
                    ; Type:Er
                        -> puts("Unknown error: ~p:~p\n", [Type, Er])
                        , Log({Type, Er})
                        , exit({Type, Er})
                    end
                ; false
                    % Not enough bytes in Candidate to try to find a term. More data is needed.
                    -> {ok, Sofar + Offset}
                end
        end
    .

found_term(Term, Log)
    -> puts("TERM (~p): ~w\n", [size(term_to_binary(Term)), Term])
    , Log({found_term, Term})
    , case Term
        of { {[]}, _Extra }
            -> ok % Do not process a totally empty object.
        ; {kv_node, [ {DocId, _Info} ]} when is_binary(DocId) andalso is_tuple(_Info)
            -> io:format("{\"_id\":\"~s\"}\n", [DocId])
        ; {kv_node, Other}
            -> Log({kv_node, Other})
        ; {Ejson, _Extra}
            -> Handler =
                fun ({L}) when is_list(L)
                    -> {struct, L}
                ; (Bad)
                    -> puts("JSON ERROR: ~p\n", [Bad])
                    , Log({handler_unknown, Bad})
                    , exit({json_encode, {bad_term, Bad}})
                end
            , try (mochijson2:encoder([{handler, Handler}]))(Ejson)
                of Json
                    %-> io:format("~s\n", [Json])
                    -> io:format([Json, <<"\n">>])
                catch Type:Er
                    -> puts("MOCHIJSON ERROR: ~p:~p\nEjson=~p\n", [Type, Er, Ejson])
                    , Log({json_encode, Term})
                end
        ; _
            -> puts("UNKNOWN TERM:\n~p\n", [Term])
            , Log({unknown_term, Term})
        end
    .

%% vim: sts=4 sw=4 et
