-module(grep_couch).
-export([main/1]).

-define(MB       , 1 * 1024 * 1024).
-define(READ_SIZE, 1 * 1024 * 1024).
%-define(T2B_MAGIC, <<131, 104, 1, 108, 0, 0>>).
%-define(MAGIC_LEN, 6).
%-define(T2B_MAGIC, <<131, 108, 0, 0>>).
%-define(MAGIC_LEN, 4).

% Looking for 2-tuples where the first element is a 1-tuple.
-define(T2B_MAGIC, <<131, 104, 2, 104, 1>>).
-define(MAGIC_LEN, 5).

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
    , json_scan(File)
    , halt(0)
    ;

main(_)
    -> usage()
    .

json_scan(File)
    -> json_scan(File, 0, <<"">>, false)
    .

json_scan(File, Offset, Pending, Eof)
    -> PendingSize = size(Pending)
    , CurrentPos = Offset - PendingSize
    , puts("Pos=~p PendingSize=~p\n", [CurrentPos, PendingSize])

    , Result = find_terms(Pending, Eof)
    , puts(" Result: ~p\n", [Result])

    , case Result
        of {error, Reason}
            -> puts("ERROR: ~p\n", [Reason])
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
                        ; eof
                            -> json_scan(File, Offset + Consumed, Remainder, true)
                        ; {ok, Data}
                            -> json_scan(File, Offset + Consumed + size(Data), <<Remainder/binary, Data/binary>>, false)
                        end
                end
        end
    .

find_terms(Data, Eof)
    -> find_terms(Data, 0, Eof)
    .

find_terms(Data, Sofar, Eof)
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
                            -> found_term(Term)
                            % Unfortunately, the only thing I know to do at
                            % this point is *re-convert* back to binary to
                            % see how long it was.
                            , TermLength = size(term_to_binary(Term))
                            , Remainder = binary:part(Candidate, {TermLength, CandidateLength - TermLength})
                            , find_terms(Remainder, Sofar + Offset + TermLength, Eof)
                    catch error:badarg
                        % Despite the magic string being found, a term is not here. Skip over the magic bytes.
                        -> puts("Sneaky data (~p):\n~p\nEND SNEAKY\n", [CandidateLength, Candidate])
                        % Probably not enough data collected.
                        , Remainder = binary:part(Candidate, {?MAGIC_LEN, CandidateLength - ?MAGIC_LEN})
                        , find_terms(Remainder, Sofar + Offset + ?MAGIC_LEN, Eof)
                    ; Type:Er
                        -> exit({Type, Er})
                        %-> puts("Unknown error: ~p:~p\n", [Type, Er])
                    end
                ; false
                    % Not enough bytes in Candidate to try to find a term. More data is needed.
                    -> {ok, Sofar + Offset}
                end
        end
    .

found_term(Term)
    -> puts("TERM (~p): ~w\n", [size(term_to_binary(Term)), Term])
    , case Term
        of {Ejson, _Extra}
            -> Handler =
                fun ({L}) when is_list(L)
                    -> {struct, L}
                ; (Bad)
                    -> puts("JSON ERROR: ~p\n", [Bad])
                    , exit({json_encode, {bad_term, Bad}})
                end
            , try (mochijson2:encoder([{handler, Handler}]))(Ejson)
                of Json
                    -> io:format("~s\n", [Json])
                catch Type:Er
                    -> puts("MOCHIJSON ERROR: ~p:~p\nEjson=~p\n", [Type, Er, Ejson])
                end
        ; _
            -> puts("UNKNOWN TERM:\n~p\n", [Term])
        end
    .

%% vim: sts=4 sw=4 et
