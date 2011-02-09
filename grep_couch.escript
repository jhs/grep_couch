#!/usr/bin/env escript
%%! -smp enable

-define(MB       , 1 * 1024 * 1024).
-define(READ_SIZE, 1 * 1024 * 1024).
%-define(T2B_MAGIC, <<131, 104, 1, 108, 0, 0>>).
%-define(MAGIC_LEN, 6).
%-define(T2B_MAGIC, <<131, 108, 0, 0>>).
%-define(MAGIC_LEN, 4).
-define(T2B_MAGIC, <<131, 104>>).
-define(MAGIC_LEN, 2).

usage()
    -> io:format("usage: grep_couchdb /path/to/some/file\n")
    , halt(1)
    .

main([])
    -> usage()
    ;

main(Args=[Path | _Rest]) when is_list(Args)
    -> io:format("Scanning ~p\n", [Path])
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
    , io:format("Pos=~p PendingSize=~p\n", [CurrentPos, PendingSize])

    , Result = find_terms(Pending, Eof)
    , io:format(" Result: ~p\n", [Result])

    , case Result
        of {error, Reason}
            -> io:format("ERROR: ~p\n", [Reason])
        ; {ok, Consumed}
            -> Remainder = binary:part(Pending, {Consumed, PendingSize - Consumed})
            , case Eof
                of true
                    % find_terms() should have consumed the entire file.
                    -> case Remainder
                        of <<"">>
                            -> io:format("Done.\n")
                        ; _
                            -> throw({bad_finish, Remainder})
                        end
                ; false
                    % find_terms() needs more data.
                    -> case file:read(File, ?READ_SIZE)
                        of {error, Reason}
                            -> io:format("ERROR reading: ~p\n", [Reason])
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
                        -> io:format("Sneaky data (~p):\n~p\nEND SNEAKY\n", [CandidateLength, Candidate])
                        % Probably not enough data collected.
                        , {ok, Sofar + Offset + ?MAGIC_LEN}
                    ; Type:Er
                        -> io:format("Unknown error: ~p:~p\n", [Type, Er])
                    end
                ; false
                    % Not enough bytes in Candidate to try to find a term. More data is needed.
                    -> {ok, Sofar + Offset}
                end
        end
    .

found_term(Term)
    -> io:format("TERM (~p): ~p\n", [size(term_to_binary(Term)), Term])
    .

%% vim: sts=4 sw=4 etk
