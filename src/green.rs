use nix::sys::mman::{mprotect, ProtFlags};
use nix::unistd::{sysconf, SysconfVar};
use std::alloc::{alloc, dealloc, Layout};
use std::collections::{HashMap, HashSet, LinkedList};
use std::ffi::c_void;
use std::ptr;

extern "C" {
    fn set_context(ctx: *mut Registers) -> u64;
    fn switch_context(ctx: *const Registers) -> !;
}

#[repr(C)]
struct Registers {
    // callee保存レジスタ
    d8: u64,
    d9: u64,
    d10: u64,
    d11: u64,
    d12: u64,
    d13: u64,
    d14: u64,
    d15: u64,
    x19: u64,
    x20: u64,
    x21: u64,
    x22: u64,
    x23: u64,
    x24: u64,
    x25: u64,
    x26: u64,
    x27: u64,
    x28: u64,

    // リンクレジスタ
    x30: u64,
    // スタックポインタ
    sp: u64,
}

impl Registers {
    fn new(sp: u64) -> Self {
        Registers {
            d8: 0,
            d9: 0,
            d10: 0,
            d11: 0,
            d12: 0,
            d13: 0,
            d14: 0,
            d15: 0,
            x19: 0,
            x20: 0,
            x21: 0,
            x22: 0,
            x23: 0,
            x24: 0,
            x25: 0,
            x26: 0,
            x27: 0,
            x28: 0,
            x30: entry_point as u64,
            sp,
        }
    }
}

// スレッド開始時に実行する関数の型
type Entry = fn();

// コンテキスト
struct Context {
    regs: Registers,      // レジスタ
    stack: *mut u8,       // スタック
    stack_layout: Layout, // スタックレイアウト
    entry: Entry,         // エントリポイント
    id: u64,              // スレッドID
}

impl Context {
    // レジスタ情報へのポインタを取得
    fn get_regs_mut(&mut self) -> *mut Registers {
        &mut self.regs as *mut Registers
    }

    fn get_regs(&self) -> *const Registers {
        &self.regs as *const Registers
    }

    fn new(func: Entry, stack_size: usize, id: u64) -> Self {
        let page_size = sysconf(SysconfVar::PAGE_SIZE).unwrap().unwrap();

        // スタック領域の確保
        let layout = Layout::from_size_align(stack_size, page_size as usize).unwrap();
        let stack = unsafe { alloc(layout) };

        // ガードページの設定
        unsafe {
            mprotect(
                stack as *mut c_void,
                page_size as usize,
                ProtFlags::PROT_NONE,
            )
            .unwrap()
        };

        // レジスタの初期化
        let regs = Registers::new(stack as u64 + stack_size as u64);

        // コンテキストの初期化
        Context {
            regs,
            stack,
            id,
            stack_layout: layout,
            entry: func,
        }
    }
}

// すべてのスレッド終了時に戻ってくる先
static mut CTX_MAIN: Option<Box<Registers>> = None;

// 不要なスタック領域
static mut UNUSED_STACK: (*mut u8, Layout) = (ptr::null_mut(), Layout::new::<u8>());

// スレッドの実行キュー
static mut CONTEXTS: LinkedList<Box<Context>> = LinkedList::new();

// スレッドIDの集合
static mut ID: *mut HashSet<u64> = ptr::null_mut();

fn get_id() -> u64 {
    loop {
        let rnd: u64 = rand::random();
        unsafe {
            if !(*ID).contains(&rnd) {
                (*ID).insert(rnd);
                return rnd;
            }
        };
    }
}

pub fn spawn(func: Entry, stack_size: usize) -> u64 {
    unsafe {
        let id = get_id();
        CONTEXTS.push_back(Box::new(Context::new(func, stack_size, id)));
        schedule();
        id
    }
}

pub fn schedule() {
    unsafe {
        // 実行可能なプロセスが自身のみであるため即座にリターン
        if CONTEXTS.len() == 1 {
            return;
        }

        // 自身のコンテキストを実行キューの最後に移動
        let mut ctx = CONTEXTS.pop_front().unwrap();
        // レジスタ保存領域へのポインタを取得
        let regs = ctx.get_regs_mut();
        CONTEXTS.push_back(ctx);

        //レジスタを保存
        if set_context(regs) == 0 {
            // 次のスレッドにコンテキストスイッチ
            let next = CONTEXTS.front().unwrap();
            switch_context(next.get_regs());
        }

        // 不要なスタック領域を削除
        rm_unused_stack();
    }
}

extern "C" fn entry_point() {
    unsafe {
        // 指定されたエントリ関数を実行
        let ctx = CONTEXTS.front().unwrap();
        (ctx.entry)();

        // 以降がスレッド終了時の後処理

        // 自身のコンテキストを取り除く
        let ctx = CONTEXTS.pop_front().unwrap();

        // スレッドIDを削除
        (*ID).remove(&ctx.id);

        // 不要なスタック領域として保存
        // この段階で解放すると､以降のコードでスタックが使えなくなる
        UNUSED_STACK = (ctx.stack, ctx.stack_layout);

        match CONTEXTS.front() {
            Some(c) => {
                // 次のスレッドにコンテキストスイッチ
                switch_context(c.get_regs());
            }
            None => {
                // すべてのスレッドが終了した場合､main関数にスレッドに戻る
                if let Some(c) = &CTX_MAIN {
                    switch_context(&**c as *const Registers);
                }
            }
        };
    }

    panic!("entry_point");
}

pub fn spawn_from_main(func: Entry, stack_size: usize) {
    unsafe {
        // すでに初期化済みならエラーとする
        if CTX_MAIN.is_some() {
            panic!("spawn_from_main is called twice");
        }

        // main関数用のコンテキストを生成
        CTX_MAIN = Some(Box::new(Registers::new(0)));
        if let Some(ctx) = &mut CTX_MAIN {
            // グローバル変数を初期化
            let mut msgs = MappedList::new();
            MESSAGES = &mut msgs as *mut MappedList<u64>;

            let mut wating = HashMap::new();
            WAITING = &mut wating as *mut HashMap<u64, Box<Context>>;

            let mut ids = HashSet::new();
            ID = &mut ids as *mut HashSet<u64>;

            // すべてのスレッド終了時の戻り先を保存
            if set_context(&mut **ctx as *mut Registers) == 0 {
                // 最初に起動するスレッドのコンテキストを生成して実行
                CONTEXTS.push_back(Box::new(Context::new(func, stack_size, get_id())));
                let first = CONTEXTS.front().unwrap();
                switch_context(first.get_regs());
            }

            // 不要なスタックを解放
            rm_unused_stack();

            // グローバル変数をクリア
            CTX_MAIN = None;
            CONTEXTS.clear();
            MESSAGES = ptr::null_mut();
            WAITING = ptr::null_mut();
            ID = ptr::null_mut();

            msgs.clear();
            wating.clear();
            ids.clear();
        }
    }
}

unsafe fn rm_unused_stack() {
    if !UNUSED_STACK.0.is_null() {
        // スタック領域の保護を解除
        mprotect(
            UNUSED_STACK.0 as *mut c_void,
            sysconf(SysconfVar::PAGE_SIZE).unwrap().unwrap() as usize,
            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
        )
        .unwrap();

        // スタック領域解放
        dealloc(UNUSED_STACK.0, UNUSED_STACK.1);
        UNUSED_STACK = (ptr::null_mut(), Layout::new::<u8>());
    }
}

struct MappedList<T> {
    map: HashMap<u64, LinkedList<T>>,
}

impl<T> MappedList<T> {
    fn new() -> Self {
        MappedList {
            map: HashMap::new(),
        }
    }

    // keyに対応するリストの最後尾に追加
    fn push_back(&mut self, key: u64, val: T) {
        if let Some(list) = self.map.get_mut(&key) {
            // 対応するリストが存在するなら追加
            list.push_back(val);
        } else {
            // 存在しない場合新たにリストを作成して追加
            let mut list = LinkedList::new();
            list.push_back(val);
            self.map.insert(key, list);
        }
    }

    // keyに対応するリストの一番前から取り出す
    fn pop_front(&mut self, key: u64) -> Option<T> {
        if let Some(list) = self.map.get_mut(&key) {
            let val = list.pop_front();
            if list.is_empty() {
                self.map.remove(&key);
            }
            return val;
        }
        None
    }

    fn clear(&mut self) {
        self.map.clear();
    }
}

// メッセージキュー
static mut MESSAGES: *mut MappedList<u64> = ptr::null_mut();

// 待機スレッド集合
static mut WAITING: *mut HashMap<u64, Box<Context>> = ptr::null_mut();

pub fn send(key: u64, msg: u64) {
    unsafe {
        // メッセージキューの最後尾に追加
        (*MESSAGES).push_back(key, msg);

        // スレッドが受信待ちの場合に実行キューに移動
        if let Some(ctx) = (*WAITING).remove(&key) {
            CONTEXTS.push_back(ctx);
        }
        schedule();
    }
}

pub fn recv() -> Option<u64> {
    unsafe {
        // スレッドIDを取得
        let key = CONTEXTS.front().unwrap().id;

        // メッセージがすでにキューにある場合即座にリターン
        if let Some(msg) = (*MESSAGES).pop_front(key) {
            return Some(msg);
        }

        // 実行可能なスレッドが他にいない場合はデッドロック
        if CONTEXTS.len() == 1 {
            panic!("deadlock");
        }

        // 実行中のスレッドを受信待ち状態に移行
        let mut ctx = CONTEXTS.pop_front().unwrap();
        let regs = ctx.get_regs_mut();
        (*WAITING).insert(key, ctx);

        // 次の実行可能なスレッドにコンテキストスイッチ
        if set_context(regs) == 0 {
            let next = CONTEXTS.front().unwrap();
            switch_context((**next).get_regs());
        }

        // 不要なスタックを削除
        rm_unused_stack();

        // 受信したメッセージを取得
        (*MESSAGES).pop_front(key)
    }
}
