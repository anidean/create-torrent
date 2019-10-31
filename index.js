const bencode = require('bencode')
const BlockStream = require('block-stream2')
const calcPieceLength = require('piece-length')
const corePath = require('path')
const FileReadStream = require('filestream/read')
const flatten = require('flatten')
const fs = require('fs')
const isFile = require('is-file')
const junk = require('junk')
const MultiStream = require('multistream')
const once = require('once')
const parallel = require('run-parallel')
const sha1 = require('simple-sha1')
const stream = require('readable-stream')

const announceList = [
  [ 'udp://tracker.leechers-paradise.org:6969' ],
  [ 'udp://tracker.coppersurfer.tk:6969' ],
  [ 'udp://tracker.opentrackr.org:1337' ],
  [ 'udp://explodie.org:6969' ],
  [ 'udp://tracker.empire-js.us:1337' ],
  [ 'wss://tracker.btorrent.xyz' ],
  [ 'wss://tracker.openwebtorrent.com' ],
  [ 'wss://tracker.fastcast.nz' ]
]

/**
 * Create a torrent.
 * @param  {string|File|FileList|Buffer|Stream|Array.<string|File|Buffer|Stream>} input
 * @param  {Object} opts
 * @param  {string=} opts.name
 * @param  {Date=} opts.creationDate
 * @param  {string=} opts.comment
 * @param  {string=} opts.createdBy
 * @param  {string=} opts.creatorPublicKey
 * @param  {string=} opts.groupPublicKey
 * @param  {Object=} opts.creatorMetaSignature  // this signature covers createdBy, creatorPublicKey, groupPublicKey, updateType, version, nextVersionId, comment, chunkMinSize, fileChunks
 * @param  {string=} opts.updateType
 * @param  {number=} opts.version
 * @param  {string=} opts.nextVersionId
 * @param  {number=} opts.chunkMinSize
 * @param  {boolean|number=} opts.private
 * @param  {number=} opts.pieceLength
 * @param  {Array.<Array.<string>>=} opts.announceList
 * @param  {Array.<string>=} opts.urlList
 * @param  {function} cb
 * @return {Buffer} buffer of .torrent file data
 */


async function createTorrent (input, opts, cb, raw=false, use_storage=false, pass_object=null) {
  if (typeof opts === 'function') [ opts, cb ] = [ cb, opts ]
  opts = opts ? Object.assign({}, opts) : {}

  _parseInput(input, opts, (err, files, singleFileTorrent) => {
    if (err) return cb(err);
    opts.singleFileTorrent = singleFileTorrent;
    onFiles(files, opts, cb, raw, use_storage, pass_object);
  })
}

function parseInput (input, opts, cb) {
  if (typeof opts === 'function') [ opts, cb ] = [ cb, opts ]
  opts = opts ? Object.assign({}, opts) : {}
  _parseInput(input, opts, cb)
}

/**
 * Parse input file and return file information.
 */
function _parseInput (input, opts, cb) {
  if (isFileList(input)) input = Array.from(input)
  if (!Array.isArray(input)) input = [ input ]

  if (input.length === 0) throw new Error('invalid input type')

  input.forEach(item => {
    if (item == null) throw new Error(`invalid input type: ${item}`)
  })

  // In Electron, use the true file path
  input = input.map(item => {
    if (isBlob(item) && typeof item.path === 'string' && typeof fs.stat === 'function') return item.path
    return item
  })

  // If there's just one file, allow the name to be set by `opts.name`
  if (input.length === 1 && typeof input[0] !== 'string' && !input[0].name) input[0].name = opts.name

  let commonPrefix = null
  input.forEach((item, i) => {
    if (typeof item === 'string') {
      return
    }

    let path = item.fullPath || item.name
    if (!path) {
      path = `Unknown File ${i + 1}`
      item.unknownName = true
    }

    item.path = path.split('/')

    // Remove initial slash
    if (!item.path[0]) {
      item.path.shift()
    }

    if (item.path.length < 2) { // No real prefix
      commonPrefix = null
    } else if (i === 0 && input.length > 1) { // The first file has a prefix
      commonPrefix = item.path[0]
    } else if (item.path[0] !== commonPrefix) { // The prefix doesn't match
      commonPrefix = null
    }
  })

  // remove junk files
  input = input.filter(item => {
    if (typeof item === 'string') {
      return true
    }
    const filename = item.path[item.path.length - 1]
    return notHidden(filename) && junk.not(filename)
  })

  if (commonPrefix) {
    input.forEach(item => {
      const pathless = (Buffer.isBuffer(item) || isReadable(item)) && !item.path
      if (typeof item === 'string' || pathless) return
      item.path.shift()
    })
  }

  if (!opts.name && commonPrefix) {
    opts.name = commonPrefix
  }

  if (!opts.name) {
    // use first user-set file name
    input.some(item => {
      if (typeof item === 'string') {
        opts.name = corePath.basename(item)
        return true
      } else if (!item.unknownName) {
        opts.name = item.path[item.path.length - 1]
        return true
      }
    })
  }

  if (!opts.name) {
    opts.name = `Unnamed Torrent ${Date.now()}`
  }

  const numPaths = input.reduce((sum, item) => sum + Number(typeof item === 'string'), 0)

  let isSingleFileTorrent = (input.length === 1)

  if (input.length === 1 && typeof input[0] === 'string') {
    if (typeof fs.stat !== 'function') {
      throw new Error('filesystem paths do not work in the browser')
    }
    // If there's a single path, verify it's a file before deciding this is a single
    // file torrent
    isFile(input[0], (err, pathIsFile) => {
      if (err) return cb(err)
      isSingleFileTorrent = pathIsFile
      processInput()
    })
  } else {
    process.nextTick(() => {
      processInput()
    })
  }

  function processInput () {
    parallel(input.map(item => cb => {
      const file = {}

      if (isBlob(item)) {
        console.log("it's a blob");
        file.getStream = getBlobStream(item)
        file.length = item.size
      } else if (Buffer.isBuffer(item)) {
        console.log("it's a buffer");
        file.getStream = getBufferStream(item)
        file.length = item.length
      } else if (isReadable(item)) {
        console.log("it's readable");
        file.getStream = getStreamStream(item, file)
        file.length = 0
      } else if (typeof item === 'string') {
        console.log("it's a string");
        if (typeof fs.stat !== 'function') {
          throw new Error('filesystem paths do not work in the browser')
        }
        const keepRoot = numPaths > 1 || isSingleFileTorrent
        getFiles(item, keepRoot, cb)
        return // early return!
      } else {
        throw new Error('invalid input type')
      }
      file.path = item.path
      cb(null, file)
    }), (err, files) => {
      if (err) return cb(err)
      files = flatten(files)
      cb(null, files, isSingleFileTorrent)
    })
  }
}

function getFiles (path, keepRoot, cb) {
  traversePath(path, getFileInfo, (err, files) => {
    if (err) return cb(err)

    if (Array.isArray(files)) files = flatten(files)
    else files = [ files ]

    path = corePath.normalize(path)
    if (keepRoot) {
      path = path.slice(0, path.lastIndexOf(corePath.sep) + 1)
    }
    if (path[path.length - 1] !== corePath.sep) path += corePath.sep

    files.forEach(file => {
      file.getStream = getFilePathStream(file.path)
      file.path = file.path.replace(path, '').split(corePath.sep)
    })
    cb(null, files)
  })
}

function getFileInfo (path, cb) {
  cb = once(cb)
  fs.stat(path, (err, stat) => {
    if (err) return cb(err)
    const info = {
      length: stat.size,
      path
    }
    cb(null, info)
  })
}

function traversePath (path, fn, cb) {
  fs.stat(path, (err, stats) => {
    if (err) return cb(err)
    if (stats.isDirectory()) {
      fs.readdir(path, (err, entries) => {
        if (err) return cb(err)
        parallel(entries.filter(notHidden).filter(junk.not).map(entry => cb => {
          traversePath(corePath.join(path, entry), fn, cb)
        }), cb)
      })
    } else if (stats.isFile()) {
      fn(path, cb)
    }
    // Ignore other types (not a file or directory)
  })
}

function notHidden (file) {
  return file[0] !== '.'
}

async function getPieceList (upload_instance, files, chunkMinSize, pieceLength, use_storage=false, cb) {
  console.log('inpiecelist');
  console.log(files);
  console.log(upload_instance);
  cb = once(cb);
  console.log("after once");
  const pieces = [];

  let length = 0;
  let total_file_length = 0;
  for (let i=0;i<files.length;i++){
    total_file_length += files[i].length;
  }
  const streams = files.map(file => file.getStream);
  let blob_key_array = [];
  let remainingHashes = 0;
  let pieceNum = 0;
  let ended = false;
  const multistream = new MultiStream(streams);
  console.log("first queue length", multistream._queue.length);
  const blockstream = new BlockStream(pieceLength, { zeroPadding: false });

  multistream.on('error', onError);

  let file_spot = 0;
  let local_blob_key = await getUniqueStorageId(blob_store);
  let chunk_blob = new ChunkBlob(local_key=local_blob_key);

  console.log("local_torrent_key", upload_instance.local_torrent_key);
  console.log("before getting stored_torrent - key:", upload_instance.local_torrent_key);
  let local_torrent_key = upload_instance.local_torrent_key;
  let stored_torrent = await AgginymTorrent.getItem(local_torrent_key);
  console.log("after get stored_torrent");

  // this is a shitty way of doing it, find a better way
  // right now we find the size of the chunks that will be created
  // add them up until just under the aggregate size on disk of all files
  // them get the left over chunk size as well.
  let main_chunk_size = chunkMinSize + pieceLength - chunkMinSize % pieceLength;
  let full_chunk_count = Math.floor(total_file_length / main_chunk_size);
  let leftover_chunk_size = total_file_length - full_chunk_count * main_chunk_size;
  let leftover_chunk = leftover_chunk_size > 0;

  let chunk_array = new Array(full_chunk_count + (leftover_chunk? 1:0));
  chunk_array.fill(main_chunk_size);
  if (leftover_chunk){
    chunk_array[chunk_array.length-1] = leftover_chunk_size;
  }

  let chunk_offset = 0;
  let chunk_spot = 0;

  multistream
    .pipe(blockstream)
    .on('data', onData)
    .on('end', onEnd)
    .on('error', onError);

  async function onData (chunk) {
    length += chunk.length;

    //console.log("multistream._queue.length", multistream._queue.length);

    // allocate chunk if needed
    if (chunk_blob.blob === null){
      chunk_blob.blob = new Uint8Array(chunk_array[chunk_spot++]);
    }

    chunk_blob.blob.set(chunk, chunk_offset);
    chunk_offset += chunk.length;
    //console.log("chunk_offset:", chunk_offset, "chunk.length:", chunk.length, "pieceLength:", pieceLength);
    //console.log("chunk_blob.blob.length: ", chunk_blob.blob.length);
    //console.log("file length: ", files[file_spot].length);

    if (chunk_offset == chunk_blob.blob.length){
      // we've filled the chunk, do the next one
      console.log("FILLED THE CHUNK");
      chunk_offset = 0;
      currently_saving.add(chunk_blob.local_key); // marking as still saving
      save_file_chunk(chunk_blob, currently_saving);
      // set up next blob
      local_blob_key = await getUniqueStorageId(blob_store);
      chunk_blob = new ChunkBlob(local_key=local_blob_key);
    }

    async function save_file_chunk(chunk_b, currently_saving){
      console.log("iterating chunks");
      //store blob
      chunk_b.setItem()
      blob_key_array.push({key: chunk_b.local_key, hash: sha1.sync(chunk_b.blob)});
      currently_saving.delete(chunk_b.local_key);  // will be empty when all pieces are done saving
      // setup next blob
      //local_blob_key = await getUniqueStorageId(blob_store);
      //chunk_blob = new ChunkBlob(local_key=local_blob_key);
    }

    const i = pieceNum;
    sha1(chunk, hash => {
      pieces[i] = hash;
      remainingHashes -= 1;
      maybeDone();
    });
    remainingHashes += 1;
    pieceNum += 1;    
  }

  async function onEnd () {
    console.log("ended");

    ended = true;
    maybeDone();
  }

  function onError (err) {
    cleanup();
    cb(err);
  }

  function cleanup () {
    multistream.removeListener('error', onError)
    blockstream.removeListener('data', onData)
    blockstream.removeListener('end', onEnd)
    blockstream.removeListener('error', onError)
  }

  function maybeDone () {
    if (ended && remainingHashes === 0) {
      cleanup();
      cb(null, Buffer.from(pieces.join(''), 'hex'), main_chunk_size, blob_key_array, length);
    }
  }
}

async function onFiles (files, opts, cb, raw=false, use_storage=false, pass_object=null) {
  let announceList = opts.announceList

  if (!announceList) {
    if (typeof opts.announce === 'string') announceList = [ [ opts.announce ] ]
    else if (Array.isArray(opts.announce)) {
      announceList = opts.announce.map(u => [ u ])
    }
  }

  if (!announceList) announceList = []

  if (global.WEBTORRENT_ANNOUNCE) {
    if (typeof global.WEBTORRENT_ANNOUNCE === 'string') {
      announceList.push([ [ global.WEBTORRENT_ANNOUNCE ] ])
    } else if (Array.isArray(global.WEBTORRENT_ANNOUNCE)) {
      announceList = announceList.concat(global.WEBTORRENT_ANNOUNCE.map(u => [ u ]))
    }
  }

  // When no trackers specified, use some reasonable defaults
  if (opts.announce === undefined && opts.announceList === undefined) {
    announceList = announceList.concat(module.exports.announceList)
  }

  if (typeof opts.urlList === 'string') opts.urlList = [ opts.urlList ]

  const torrent = {
    info: {
      name: opts.name
    },
    'creation date': Math.ceil((Number(opts.creationDate) || Date.now()) / 1000),
    encoding: 'UTF-8'
  }

  if (announceList.length !== 0) {
    torrent.announce = announceList[0][0]
    torrent['announce-list'] = announceList
  }

  if (opts.comment !== undefined) torrent.comment = opts.comment

  if (opts.createdBy !== undefined) torrent['created by'] = opts.createdBy

  if (opts.private !== undefined) torrent.info.private = Number(opts.private)

  // "ssl-cert" key is for SSL torrents, see:
  //   - http://blog.libtorrent.org/2012/01/bittorrent-over-ssl/
  //   - http://www.libtorrent.org/manual-ref.html#ssl-torrents
  //   - http://www.libtorrent.org/reference-Create_Torrents.html
  if (opts.sslCert !== undefined) torrent.info['ssl-cert'] = opts.sslCert

  if (opts.urlList !== undefined) torrent['url-list'] = opts.urlList

  // Agginym Specific stuff
  if (opts.creatorPublicKey !== undefined) torrent['creator_public_key'] = opts.creatorPublicKey; // key for verifying updates
  if (opts.groupPublicKey !== undefined) torrent['group_public_key'] = opts.groupPublicKey;  // only those in the group (sig verified) can share
  if (opts.updateType !== undefined) torrent['update type'] = opts.updateType; // append: reference old files in new torrent, replace: next version, no references, none: no updates
  if (opts.versionNumber !== undefined) torrent['version number'] = opts.versionNumber; // versions are ints and only go up.
  if (opts.newVersionId !== undefined) torrent['new version id'] = opts.newVersionId; // agginym lookup id for the new version
  if (opts.creatorMetaSignature !== undefined) torrent['meta signature'] = opts.creatorMetaSignature; // make sure the meta doesn't change.  necessary for agginym features.  many leave comment alone?
  if (opts.chunkMinSize !== undefined) torrent['chunk minimum size'] = opts.chunkMinSize;
  if (opts.groupFingerprint !== undefined) torrent['group_fingerprint'] = opts.groupFingerprint;
  
  const pieceLength = opts.pieceLength || calcPieceLength(files.reduce(sumLength, 0));
  const chunkMinSize = opts.chunkMinSize;
  torrent.info['piece length'] = pieceLength;

  let password = genpass();
  let outer_password = genpass();

  let local_torrent_key = await getUniqueStorageId(torrent_store);

  let upload_key = await getUniqueStorageId(upload_store);

  console.log("local torrent key");
  console.log(local_torrent_key);
  let agginym_torrent = new AgginymTorrent(
    obj={},
    local_key=local_torrent_key,
    local_upload_keys=[upload_key],
    pass=password,
    outer_pass=outer_password,
    uploader=opts.creatorPublicKey
  );
  console.log("setting torrent");
  console.log(AgginymTorrent.store);
  console.log(agginym_torrent.store);
  await agginym_torrent.setItem();
  console.log("done setting torrent");
  // get all agginym handles
  let agginym_files = [];


  for(f of files){
    let thekey = await getUniqueStorageId(file_store);
    console.log("%%%%%%%%%%%%%%%%%");
    console.log("NEW FILE KEY", thekey);
    let thepath = f.path[0];
    console.log("path:", f.path);
    let agginym_file = new AgginymFile(
        obj={},
        local_key=thekey,
        name=thepath,
        local_torrent_keys=[local_torrent_key], //agginym_torrent.local_key
        local_upload_keys=[upload_key],
        circle_fingerprint=opts.groupFingerprint,
        chunks = new Array()
      );
    console.log(agginym_file);
    await agginym_file.setItem();
    console.log(agginym_file);
    console.log("%%%%%%%%%%%%%%%%%");
    agginym_files.push(agginym_file);
  }

  console.log("LOCALTORRENTKEY");
  console.log(local_torrent_key);
  let upload_instance = new UploadInstance(
    obj={},
    local_key=upload_key,
    circle_fingerprint=opts.groupFingerprint,
    local_file_keys=agginym_files.map(fi => fi.local_key),
    local_torrent_key=local_torrent_key
  );

  console.log("***");
  upload_instance.local_torrent_key = local_torrent_key;
  console.log(agginym_files);
  console.log(upload_instance);
  console.log(agginym_torrent);
  console.log("***");
  await upload_instance.setItem();

  console.log("files:", files);
  console.log("upload_instance:", upload_instance);
  let currently_saving = new Set();
  getPieceList(upload_instance, files, chunkMinSize, pieceLength, use_storage, async (err, pieces, main_chunk_size, blob_key_array, torrentLength) => {
    if (err) return cb(err);
    torrent.info.pieces = pieces;
    torrent.fileChunks = []; // this holds all of the chunks in objects '{name:,chunk_list:[{}]}'
    torrent.pieceLength = pieceLength;
    torrent.chunkSize = main_chunk_size;
    let chunk_bytes_seen = 0;
    let file_bytes_seen = 0;
    let file_bytes_matched = 0;
    let file_count = 0;
    console.log("blob_key_array", blob_key_array);
    let startwaittime = new Date.getTime();
    while(currently_saving.size > 0){
      let endwaittime = new Date.getTime();
      console.log(`${endwaittime-startwaittime/1000} seconds have passed, ${currently_saving.size} chunks are still being loaded into aggynym system`);
      console.log(currently_saving);
    }
    for(let blob_data of blob_key_array){
      console.log("blob_data", blob_data);
      // create the FileChunks for the blobs.
      let local_chunk_key = await getUniqueStorageId(chunk_store);
      let file_chunk = new FileChunk(
        obj={},
        local_key=local_chunk_key,
        local_torrent_key=upload_instance.local_torrent_key,
        local_blob_key=blob_data.key,
        local_file_keys=new Array(),
        hash = blob_data.hash,
        password=agginym_torrent.password,
        outer_password=agginym_torrent.outer_password,
        start_piece=Math.floor(chunk_bytes_seen / pieceLength),
        end_piece=Math.floor((main_chunk_size+chunk_bytes_seen) / pieceLength)
      );
      chunk_bytes_seen += main_chunk_size;
      if (!Array.isArray(file_chunk.local_file_keys)){
        file_chunk.local_file_keys = new Array();
      }
      // Match up Files and FileChunks
      while(file_count < files.length){
        if (chunk_bytes_seen >= file_bytes_seen + files[file_count].length){
          let stored_file = await AgginymFile.getItem(upload_instance.local_file_keys[file_count]);
          //if (!Array.isArray)
          file_chunk.local_file_keys.push(stored_file.local_key);
          stored_file.chunks.push(file_chunk.local_key);
          console.log(stored_file);
          await stored_file.setItem();
          file_bytes_seen += files[file_count].length;
          ++file_count;
        }
        else if(chunk_bytes_seen < file_bytes_seen + files[file_count].length){
          let stored_file = await AgginymFile.getItem(upload_instance.local_file_keys[file_count]);
          console.log("stored_file:::", stored_file);
          file_chunk.local_file_keys.push(stored_file.local_key);
          console.log(stored_file instanceof AgginymFile);
          stored_file.chunks.push(file_chunk.local_key);
          console.log(stored_file);
          await stored_file.setItem();
          break;
        }
      }

      await file_chunk.setItem();
    }


    files.forEach(file => {
      delete file.getStream;
    });

    if (opts.singleFileTorrent) {
      torrent.info.length = torrentLength;
    } else {
      torrent.info.files = files;
    }

    agginym_torrent.raw = torrent;
    await agginym_torrent.setItem();

    if (raw){
      console.log("raw is true");

      cb(null, upload_instance, pass_object); // the torrent will be encoded after uploading.
    } 
    else{
      cb(null, bencode.encode(torrent));
    }
  });
}

/**
 * Accumulator to sum file lengths
 * @param  {number} sum
 * @param  {Object} file
 * @return {number}
 */
function sumLength (sum, file) {
  return sum + file.length
}

/**
 * Check if `obj` is a W3C `Blob` object (which `File` inherits from)
 * @param  {*} obj
 * @return {boolean}
 */
function isBlob (obj) {
  return typeof Blob !== 'undefined' && obj instanceof Blob
}

/**
 * Check if `obj` is a W3C `FileList` object
 * @param  {*} obj
 * @return {boolean}
 */
function isFileList (obj) {
  return typeof FileList !== 'undefined' && obj instanceof FileList
}

/**
 * Check if `obj` is a node Readable stream
 * @param  {*} obj
 * @return {boolean}
 */
function isReadable (obj) {
  return typeof obj === 'object' && obj != null && typeof obj.pipe === 'function'
}

/**
 * Convert a `File` to a lazy readable stream.
 * @param  {File|Blob} file
 * @return {function}
 */
function getBlobStream (file) {
  return () => new FileReadStream(file)
}

/**
 * Convert a `Buffer` to a lazy readable stream.
 * @param  {Buffer} buffer
 * @return {function}
 */
function getBufferStream (buffer) {
  return () => {
    const s = new stream.PassThrough()
    s.end(buffer)
    return s
  }
}

/**
 * Convert a file path to a lazy readable stream.
 * @param  {string} path
 * @return {function}
 */
function getFilePathStream (path) {
  return () => fs.createReadStream(path)
}

/**
 * Convert a readable stream to a lazy readable stream. Adds instrumentation to track
 * the number of bytes in the stream and set `file.length`.
 *
 * @param  {Stream} stream
 * @param  {Object} file
 * @return {function}
 */
function getStreamStream (readable, file) {
  return () => {
    const counter = new stream.Transform()
    counter._transform = function (buf, enc, done) {
      file.length += buf.length
      this.push(buf)
      done()
    }
    readable.pipe(counter)
    return counter
  }
}

module.exports = {createTorrent: createTorrent};
module.exports.FileReadStream = FileReadStream;
module.exports.Bencode = bencode.encode;
module.exports.Dencode = bencode.decode;
module.exports.parseInput = parseInput;
module.exports.announceList = announceList;
