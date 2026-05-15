{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, DoAndIfThenElse, CPP #-}

-- | This module provides a way in which the Haskell standard input may be forwarded to the IPython
-- frontend and thus allows the notebook to use the standard input.
--
-- This relies on the implementation of file handles in GHC, and is generally unsafe and terrible.
-- However, it is difficult to find another way to do it, as file handles are generally meant to
-- point to streams and files, and not networked communication protocols.
--
-- Before using this module, the host (non-GHC) side must hand the stdin channels off via
-- @installStdinChannels@. These channels are populated by 'serveProfile', which eagerly binds the
-- stdin ROUTER socket so that frontends which block on the TCP connect (e.g. Zed's pure-Rust
-- zmq.rs) can complete their handshake immediately.
--
-- The module must also know what @execute_request@ message is currently being replied to (which
-- will request the input). Every time the language kernel receives an @execute_request@ message
-- it should inform this module via @recordParentHeader@, so that the module may generate messages
-- with an appropriate parent header set. If this is not done, the IPython frontends will not
-- recognize the target of the communication.
--
-- Finally, in order to activate this module, @fixStdin@ must be called once. Note that if this is
-- being used from within the GHC API, @fixStdin@ /must/ be called from within the GHC session not
-- from the host code.
module IHaskell.IPython.Stdin (fixStdin, installStdinChannels, recordParentHeader) where

import           IHaskellPrelude

import           Control.Concurrent
import           GHC.IO.Handle
import           GHC.IO.Handle.Types
import           System.FilePath ((</>))
#ifdef mingw32_HOST_OS
import           System.Process (createPipe)
#else
import           System.Posix.IO
#endif
import           System.IO.Unsafe

import           IHaskell.IPython.Types
import           IHaskell.IPython.ZeroMQ
import           IHaskell.IPython.Message.UUID as UUID

-- The stdin request/reply channels backing the ROUTER socket bound by
-- 'serveProfile'. Populated by 'installStdinChannels' on the host side; read
-- by 'getInputLine' inside the GHC session. Both sides live in the same OS
-- process, so the top-level MVar is shared.
stdinChannels :: MVar (Chan Message, Chan Message)
{-# NOINLINE stdinChannels #-}
stdinChannels = unsafePerformIO newEmptyMVar

-- | Hand the stdin channels (from 'ZeroMQInterface') to this module. Call
-- once from the host process before starting the GHC session that will run
-- 'fixStdin'.
installStdinChannels :: Chan Message -> Chan Message -> IO ()
installStdinChannels req rep = putMVar stdinChannels (req, rep)

-- | Manipulate standard input so that it is sourced from the IPython frontend. This function is
-- build on layers of deep magical hackery, so be careful modifying it.
fixStdin :: String -> IO ()
fixStdin dir = void $ forkIO $ stdinOnce dir

stdinOnce :: String -> IO ()
stdinOnce dir = do
  -- Create a pipe using and turn it into handles.
#ifdef mingw32_HOST_OS
  (newStdin, stdinInput) <- createPipe
#else
  (readEnd, writeEnd) <- createPipe
  newStdin <- fdToHandle readEnd
  stdinInput <- fdToHandle writeEnd
#endif
  hSetBuffering newStdin NoBuffering
  hSetBuffering stdinInput NoBuffering

  -- Store old stdin and swap in new stdin.
  oldStdin <- hDuplicate stdin
  hDuplicateTo newStdin stdin

  loop stdinInput oldStdin newStdin

  where
    loop stdinInput oldStdin newStdin = do
      let FileHandle _ mvar = stdin
      threadDelay $ 150 * 1000
      e <- isEmptyMVar mvar
      if not e
        then loop stdinInput oldStdin newStdin
        else do
          line <- getInputLine dir
          hPutStr stdinInput $ line ++ "\n"
          loop stdinInput oldStdin newStdin

-- | Get a line of input from the IPython frontend.
getInputLine :: String -> IO String
getInputLine dir = do
  (req, rep) <- readMVar stdinChannels

  -- Send a request for input.
  uuid <- UUID.random
  let fpath = dir </> ".last-req-header"
  parentHdr <- fromMaybe (error $ "getInputLine: Failed reading " ++ fpath)
                . readMay <$> readFile fpath
  let hdr = MessageHeader (mhIdentifiers parentHdr) (Just parentHdr) mempty
              uuid (mhSessionId parentHdr) (mhUsername parentHdr) InputRequestMessage
              []
  let msg = RequestInput hdr ""
  writeChan req msg

  -- Get the reply.
  InputReply _ value <- readChan rep
  return value

recordParentHeader :: String -> MessageHeader -> IO ()
recordParentHeader dir hdr =
  writeFile (dir ++ "/.last-req-header") $ show hdr
